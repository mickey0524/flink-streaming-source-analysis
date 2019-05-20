/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.network.api.writer;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.api.serialization.RecordSerializer;
import org.apache.flink.runtime.io.network.api.serialization.SpanningRecordSerializer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.util.XORShiftRandom;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;
import java.util.Random;

import static org.apache.flink.runtime.io.network.api.serialization.RecordSerializer.SerializationResult;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A record-oriented runtime result writer.
 *
 * <p>The RecordWriter wraps the runtime's {@link ResultPartitionWriter} and takes care of
 * serializing records into buffers.
 *
 * <p><strong>Important</strong>: it is necessary to call {@link #flushAll()} after
 * all records have been written with {@link #emit(IOReadableWritable)}. This
 * ensures that all produced records are written to the output stream (incl.
 * partially filled ones).
 *
 * @param <T> the type of the record that can be emitted with this record writer
 */
public class RecordWriter<T extends IOReadableWritable> {

	private static final Logger LOG = LoggerFactory.getLogger(RecordWriter.class);

	private final ResultPartitionWriter targetPartition;

	private final ChannelSelector<T> channelSelector;

	private final int numberOfChannels;

	private final int[] broadcastChannels;

	private final RecordSerializer<T> serializer;

	private final Optional<BufferBuilder>[] bufferBuilders;

	private final Random rng = new XORShiftRandom();

	private Counter numBytesOut = new SimpleCounter();

	private Counter numBuffersOut = new SimpleCounter();

	private final boolean flushAlways;

	/** Default name for teh output flush thread, if no name with a task reference is given. */
	private static final String DEFAULT_OUTPUT_FLUSH_THREAD_NAME = "OutputFlusher";

	/** The thread that periodically flushes the output, to give an upper latency bound. */
	private final Optional<OutputFlusher> outputFlusher;

	/** To avoid synchronization overhead on the critical path, best-effort error tracking is enough here.*/
	private Throwable flusherException;

	public RecordWriter(ResultPartitionWriter writer) {
		this(writer, new RoundRobinChannelSelector<T>(), -1, null);
	}

	public RecordWriter(
			ResultPartitionWriter writer,
			ChannelSelector<T> channelSelector,
			long timeout,
			String taskName) {
		this.targetPartition = writer;
		this.channelSelector = channelSelector;
		this.numberOfChannels = writer.getNumberOfSubpartitions();
		this.channelSelector.setup(numberOfChannels);

		this.serializer = new SpanningRecordSerializer<T>();
		this.bufferBuilders = new Optional[numberOfChannels];
		this.broadcastChannels = new int[numberOfChannels];
		for (int i = 0; i < numberOfChannels; i++) {
			broadcastChannels[i] = i;
			bufferBuilders[i] = Optional.empty();
		}

		checkArgument(timeout >= -1);
		this.flushAlways = (timeout == 0);  // bufferTimeout 为 0，代表不缓存，每一条记录直接 flush
		// 当 timeout 为 -1 的时候，按照 env 那里设置的 bufferTimeout 来
		// 默认情况下，100 ms
		if (timeout == -1 || timeout == 0) {
			outputFlusher = Optional.empty();
		} else {
			// 如果设置了 bufferTimeout，将启动一个线程
			// Thread.sleep 等待对应的时间
			// 然后 flushAll
			String threadName = taskName == null ?
				DEFAULT_OUTPUT_FLUSH_THREAD_NAME :
				DEFAULT_OUTPUT_FLUSH_THREAD_NAME + " for " + taskName;

			outputFlusher = Optional.of(new OutputFlusher(threadName, timeout));
			outputFlusher.get().start();
		}
	}

	/**
	 * output.collect() 真正调用的方法
	 */
	public void emit(T record) throws IOException, InterruptedException {
		checkErroneous();
		emit(record, channelSelector.selectChannel(record));
	}

	/**
	 * This is used to broadcast Streaming Watermarks in-band with records. This ignores
	 * the {@link ChannelSelector}.
	 */
	/**
	 * 用来广播 record，忽略 ChannelSelector
	 */
	public void broadcastEmit(T record) throws IOException, InterruptedException {
		checkErroneous();
		serializer.serializeRecord(record);

		boolean pruneAfterCopying = false;
		for (int channel : broadcastChannels) {
			if (copyFromSerializerToTargetChannel(channel)) {
				pruneAfterCopying = true;
			}
		}

		// Make sure we don't hold onto the large intermediate serialization buffer for too long
		// 确保我们不会长时间保留大型中间序列化缓冲区
		if (pruneAfterCopying) {
			serializer.prune();
		}
	}

	/**
	 * This is used to send LatencyMarks to a random target channel.
	 */
	/**
	 * 用来将延迟 mark 发送到任意一个目标 channel
	 */
	public void randomEmit(T record) throws IOException, InterruptedException {
		emit(record, rng.nextInt(numberOfChannels));
	}

	/**
	 * 将 record 发送到指定的 targetChannel 通道
	 */
	private void emit(T record, int targetChannel) throws IOException, InterruptedException {
		serializer.serializeRecord(record);

		if (copyFromSerializerToTargetChannel(targetChannel)) {
			serializer.prune();
		}
	}

	/**
	 * @param targetChannel
	 * @return <tt>true</tt> if the intermediate serialization buffer should be pruned
	 */
	/**
	 * 将序列化之后的 record 拷贝到目标 channel
	 * 返回是否应该修剪中间序列化缓冲区
	 */
	private boolean copyFromSerializerToTargetChannel(int targetChannel) throws IOException, InterruptedException {
		// We should reset the initial position of the intermediate serialization buffer before
		// copying, so the serialization results can be copied to multiple target buffers.
		// 我们应该在复制之前重置中间序列化缓冲区的初始位置，这样可以将序列化结果复制到多个目标缓冲区
		serializer.reset();

		boolean pruneTriggered = false;
		BufferBuilder bufferBuilder = getBufferBuilder(targetChannel);
		SerializationResult result = serializer.copyToBufferBuilder(bufferBuilder);
		while (result.isFullBuffer()) {
			numBytesOut.inc(bufferBuilder.finish());
			numBuffersOut.inc();

			// If this was a full record, we are done. Not breaking out of the loop at this point
			// will lead to another buffer request before breaking out (that would not be a
			// problem per se, but it can lead to stalls in the pipeline).
			// 如果这是一个完整的记录，我们就完成了
			// 此时不断开循环将在爆发之前导致另一个缓冲请求（这本身不会有问题，但它可能导致管道中的停顿）
			if (result.isFullRecord()) {
				pruneTriggered = true;
				bufferBuilders[targetChannel] = Optional.empty();
				break;
			}

			bufferBuilder = requestNewBufferBuilder(targetChannel);
			result = serializer.copyToBufferBuilder(bufferBuilder);
		}
		checkState(!serializer.hasSerializedData(), "All data should be written at once");

		if (flushAlways) {
			targetPartition.flush(targetChannel);
		}
		return pruneTriggered;
	}

	public void broadcastEvent(AbstractEvent event) throws IOException {
		try (BufferConsumer eventBufferConsumer = EventSerializer.toBufferConsumer(event)) {
			for (int targetChannel = 0; targetChannel < numberOfChannels; targetChannel++) {
				tryFinishCurrentBufferBuilder(targetChannel);

				// Retain the buffer so that it can be recycled by each channel of targetPartition
				targetPartition.addBufferConsumer(eventBufferConsumer.copy(), targetChannel);
			}

			if (flushAlways) {
				flushAll();
			}
		}
	}

	/**
	 * writer flush 所有记录的操作
	 */
	public void flushAll() {
		targetPartition.flushAll();
	}

	/**
	 * 清空所有的 buffer
	 */
	public void clearBuffers() {
		for (int targetChannel = 0; targetChannel < numberOfChannels; targetChannel++) {
			closeBufferBuilder(targetChannel);
		}
	}

	/**
	 * Sets the metric group for this RecordWriter.
     */
	public void setMetricGroup(TaskIOMetricGroup metrics) {
		numBytesOut = metrics.getNumBytesOutCounter();
		numBuffersOut = metrics.getNumBuffersOutCounter();
	}

	/**
	 * Marks the current {@link BufferBuilder} as finished and clears the state for next one.
	 */
	private void tryFinishCurrentBufferBuilder(int targetChannel) {
		if (!bufferBuilders[targetChannel].isPresent()) {
			return;
		}
		BufferBuilder bufferBuilder = bufferBuilders[targetChannel].get();
		bufferBuilders[targetChannel] = Optional.empty();
		numBytesOut.inc(bufferBuilder.finish());
		numBuffersOut.inc();
	}

	/**
	 * The {@link BufferBuilder} may already exist if not filled up last time, otherwise we need
	 * request a new one for this target channel.
	 */
	/**
	 * 如果上次没有填满，BufferBuilder 可能已经存在，否则我们需要为此目标通道请求一个新的
	 */
	private BufferBuilder getBufferBuilder(int targetChannel) throws IOException, InterruptedException {
		if (bufferBuilders[targetChannel].isPresent()) {
			return bufferBuilders[targetChannel].get();
		} else {
			return requestNewBufferBuilder(targetChannel);
		}
	}

	/**
	 * 申请一个新的 BufferBuilder
	 */
	private BufferBuilder requestNewBufferBuilder(int targetChannel) throws IOException, InterruptedException {
		// 要么该 targetChannel 目前没有 BufferBuilder，要么 targetChannel 的 BufferBuilder 完成了
		checkState(!bufferBuilders[targetChannel].isPresent() || bufferBuilders[targetChannel].get().isFinished());

		BufferBuilder bufferBuilder = targetPartition.getBufferProvider().requestBufferBuilderBlocking();
		bufferBuilders[targetChannel] = Optional.of(bufferBuilder);
		targetPartition.addBufferConsumer(bufferBuilder.createBufferConsumer(), targetChannel);
		return bufferBuilder;
	}

	/**
	 * 将目前被使用的 channel 关闭，清空
	 */
	private void closeBufferBuilder(int targetChannel) {
		if (bufferBuilders[targetChannel].isPresent()) {
			bufferBuilders[targetChannel].get().finish();
			bufferBuilders[targetChannel] = Optional.empty();
		}
	}

	/**
	 * Closes the writer. This stops the flushing thread (if there is one).
	 */
	public void close() {
		clearBuffers();
		// make sure we terminate the thread in any case
		// 确保我们在任何情况下都关闭了线程
		if (outputFlusher.isPresent()) {
			outputFlusher.get().terminate();
			try {
				outputFlusher.get().join();
			} catch (InterruptedException e) {
				// ignore on close
				// restore interrupt flag to fast exit further blocking calls
				Thread.currentThread().interrupt();
			}
		}
	}

	/**
	 * Notifies the writer that the output flusher thread encountered an exception.
	 *
	 * @param t The exception to report.
	 */
	/**
	 * 更新 flusherException，表明定时 flush 线程遇到了异常
	 */
	private void notifyFlusherException(Throwable t) {
		if (flusherException == null) {
			LOG.error("An exception happened while flushing the outputs", t);
			flusherException = t;
		}
	}

	/**
	 * 检查当前是否存在异常
	 */
	private void checkErroneous() throws IOException {
		if (flusherException != null) {
			throw new IOException("An exception happened while flushing the outputs", flusherException);
		}
	}

	/**
	 * 静态方法
	 * 创建一个 RecordWriter
	 */
	public static RecordWriter createRecordWriter(
			ResultPartitionWriter writer,
			ChannelSelector channelSelector,
			long timeout,
			String taskName) {
		// channelSelector 就是设置的各种 Partitioner
		// 如果是 BroadcastPartitioner，返回 BroadcastRecordWriter
		if (channelSelector.isBroadcast()) {
			return new BroadcastRecordWriter<>(writer, channelSelector, timeout, taskName);
		} else {
			return new RecordWriter<>(writer, channelSelector, timeout, taskName);
		}
	}

	public static RecordWriter createRecordWriter(
			ResultPartitionWriter writer,
			ChannelSelector channelSelector,
			String taskName) {
		return createRecordWriter(writer, channelSelector, -1, taskName);
	}

	// ------------------------------------------------------------------------


	/**
	 * A dedicated thread that periodically flushes the output buffers, to set upper latency bounds.
	 *
	 * <p>The thread is daemonic, because it is only a utility thread.
	 */
	private class OutputFlusher extends Thread {

		private final long timeout;

		private volatile boolean running = true;

		OutputFlusher(String name, long timeout) {
			super(name);
			setDaemon(true);
			this.timeout = timeout;
		}

		public void terminate() {
			running = false;
			interrupt();
		}

		@Override
		public void run() {
			try {
				while (running) {
					try {
						Thread.sleep(timeout);
					} catch (InterruptedException e) {
						// propagate this if we are still running, because it should not happen
						// in that case
						if (running) {
							throw new Exception(e);
						}
					}

					// any errors here should let the thread come to a halt and be
					// recognized by the writer
					flushAll();
				}
			} catch (Throwable t) {
				notifyFlusherException(t);
			}
		}
	}
}
