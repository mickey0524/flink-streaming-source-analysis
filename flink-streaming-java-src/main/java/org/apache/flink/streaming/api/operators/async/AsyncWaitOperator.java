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

package org.apache.flink.streaming.api.operators.async;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.AsyncDataStream.OutputMode;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.async.queue.OrderedStreamElementQueue;
import org.apache.flink.streaming.api.operators.async.queue.StreamElementQueue;
import org.apache.flink.streaming.api.operators.async.queue.StreamElementQueueEntry;
import org.apache.flink.streaming.api.operators.async.queue.StreamRecordQueueEntry;
import org.apache.flink.streaming.api.operators.async.queue.UnorderedStreamElementQueue;
import org.apache.flink.streaming.api.operators.async.queue.WatermarkQueueEntry;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * The {@link AsyncWaitOperator} allows to asynchronously process incoming stream records. For that
 * the operator creates an {@link ResultFuture} which is passed to an {@link AsyncFunction}.
 * Within the async function, the user can complete the async collector arbitrarily. Once the async
 * collector has been completed, the result is emitted by the operator's emitter to downstream
 * operators.
 *
 * AsyncWaitOperator 允许异步处理传入的流记录
 * 为此，运算符创建一个 ResultFuture，并将其传递给 AsyncFunction
 * 在异步函数中，用户可以随时完成异步收集器
 * 异步收集器完成后，运算符的 emitter 将结果发送给下游运算符
 * 
 * <p>The operator offers different output modes depending on the chosen
 * {@link OutputMode}. In order to give exactly once processing guarantees, the
 * operator stores all currently in-flight {@link StreamElement} in it's operator state. Upon
 * recovery the recorded set of stream elements is replayed.
 * 
 * 操作符根据输入的 OutputMode 提供不同的输出模式
 * 为了保证 exactly once，操作符将所有的 StreamElement 存储在操作符的 state 中
 * 检查点恢复的时候，能够得到完整的元素集
 *
 * <p>In case of chaining of this operator, it has to be made sure that the operators in the chain are
 * opened tail to head. The reason for this is that an opened {@link AsyncWaitOperator} starts
 * already emitting recovered {@link StreamElement} to downstream operators.
 *
 * 如果链接该运算符，则必须确保链中的运算符是从头到尾打开的
 * 原因是打开的 AsyncWaitOperator 已经开始向下游运营商发送已恢复的 StreamElement
 * 
 * @param <IN> Input type for the operator.
 * @param <OUT> Output type for the operator.
 */
@Internal
public class AsyncWaitOperator<IN, OUT>
		extends AbstractUdfStreamOperator<OUT, AsyncFunction<IN, OUT>>
		implements OneInputStreamOperator<IN, OUT>, OperatorActions {
	private static final long serialVersionUID = 1L;
	
	// 操作符状态 name
	private static final String STATE_NAME = "_async_wait_operator_state_";

	/** Capacity of the stream element queue. */
	// 流元素队列的容量
	private final int capacity;

	/** Output mode for this operator. */
	// 操作符的输出模式，是否保证输入的顺序
	private final AsyncDataStream.OutputMode outputMode;

	/** Timeout for the async collectors. */
	// 异步收集器的 timeout
	private final long timeout;

	// 锁
	protected transient Object checkpointingLock;

	/** {@link TypeSerializer} for inputs while making snapshots. */
	// 当生成快照的时候，输入的类型序列器
	private transient StreamElementSerializer<IN> inStreamElementSerializer;

	/** Recovered input stream elements. */
	// 恢复的输入流元素
	private transient ListState<StreamElement> recoveredStreamElements;

	/** Queue to store the currently in-flight stream elements into. */
	// 存储当前 in-flight 的元素的队列
	private transient StreamElementQueue queue;

	/** Pending stream element which could not yet added to the queue. */
	// 尚未添加到队列中的待处理元素
	private transient StreamElementQueueEntry<?> pendingStreamElementQueueEntry;

	private transient ExecutorService executor;

	/** Emitter for the completed stream element queue entries. */
	// 已经完成的流元素队列的 emitter
	private transient Emitter<OUT> emitter;

	/** Thread running the emitter. */
	// 执行 emitter 的线程
	private transient Thread emitterThread;

	public AsyncWaitOperator(
			AsyncFunction<IN, OUT> asyncFunction,
			long timeout,
			int capacity,
			AsyncDataStream.OutputMode outputMode) {
		super(asyncFunction);
		chainingStrategy = ChainingStrategy.ALWAYS;

		Preconditions.checkArgument(capacity > 0, "The number of concurrent async operation should be greater than 0.");
		this.capacity = capacity;

		this.outputMode = Preconditions.checkNotNull(outputMode, "outputMode");

		this.timeout = timeout;
	}

	@Override
	public void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<OUT>> output) {
		super.setup(containingTask, config, output);

		this.checkpointingLock = getContainingTask().getCheckpointLock();

		this.inStreamElementSerializer = new StreamElementSerializer<>(
			getOperatorConfig().<IN>getTypeSerializerIn1(getUserCodeClassloader()));

		// create the operators executor for the complete operations of the queue entries
		// 为队列实体的完整操作创建运算符执行程序
		this.executor = Executors.newSingleThreadExecutor();

		switch (outputMode) {
			case ORDERED:
				queue = new OrderedStreamElementQueue(
					capacity,
					executor,
					this);
				break;
			case UNORDERED:
				queue = new UnorderedStreamElementQueue(
					capacity,
					executor,
					this);
				break;
			default:
				throw new IllegalStateException("Unknown async mode: " + outputMode + '.');
		}
	}

	@Override
	public void open() throws Exception {
		super.open();

		// create the emitter
		// 创建 emitter
		this.emitter = new Emitter<>(checkpointingLock, output, queue, this);

		// start the emitter thread
		// 开始 emitter 线程，emitter 实现了 Runnable 接口 
		this.emitterThread = new Thread(emitter, "AsyncIO-Emitter-Thread (" + getOperatorName() + ')');
		emitterThread.setDaemon(true);  // 设为常驻线程
		emitterThread.start();  // 启动 emitter 线程

		// process stream elements from state, since the Emit thread will start as soon as all
		// elements from previous state are in the StreamElementQueue, we have to make sure that the
		// order to open all operators in the operator chain proceeds from the tail operator to the
		// head operator.
		// 处理来自 state 的快照中保存的元素，因为 emit 线程将在前一个状态的元素全部插入 StreamElementQueue
		// 的时候启动，我们必须保证操作链中的操作符从 tail 到 head 启动
		if (recoveredStreamElements != null) {
			for (StreamElement element : recoveredStreamElements.get()) {
				if (element.isRecord()) {
					processElement(element.<IN>asRecord());
				}
				else if (element.isWatermark()) {
					processWatermark(element.asWatermark());
				}
				else if (element.isLatencyMarker()) {
					processLatencyMarker(element.asLatencyMarker());
				}
				else {
					throw new IllegalStateException("Unknown record type " + element.getClass() +
						" encountered while opening the operator.");
				}
			}
			// 处理完毕，将 recoveredStreamElements 赋值为空
			recoveredStreamElements = null;
		}

	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		// 用 StreamRecordQueueEntry 包裹 StreamRecord
		final StreamRecordQueueEntry<OUT> streamRecordBufferEntry = new StreamRecordQueueEntry<>(element);

		if (timeout > 0L) {
			// register a timeout for this AsyncStreamRecordBufferEntry
			// 注册一个 timeoutTimestamp 的进程时间定时器
			long timeoutTimestamp = timeout + getProcessingTimeService().getCurrentProcessingTime();

			final ScheduledFuture<?> timerFuture = getProcessingTimeService().registerTimer(
				timeoutTimestamp,
				new ProcessingTimeCallback() {
					@Override
					public void onProcessingTime(long timestamp) throws Exception {
						// 用户函数处理 timeout
						userFunction.timeout(element.getValue(), streamRecordBufferEntry);
					}
				});

			// Cancel the timer once we've completed the stream record buffer entry. This will remove
			// the register trigger task
			// 我们完成了这个 StreamRecordQueueEntry，因此取消定时器
			// cancel 操作会取消设置的定时器
			streamRecordBufferEntry.onComplete(
				(StreamElementQueueEntry<Collection<OUT>> value) -> {
					timerFuture.cancel(true);
				},
				executor);
		}

		addAsyncBufferEntry(streamRecordBufferEntry);

		userFunction.asyncInvoke(element.getValue(), streamRecordBufferEntry);
	}

	@Override
	public void processWatermark(Watermark mark) throws Exception {
		WatermarkQueueEntry watermarkBufferEntry = new WatermarkQueueEntry(mark);

		addAsyncBufferEntry(watermarkBufferEntry);
	}

	@Override
	// 将队列中的元素保存至快照
	public void snapshotState(StateSnapshotContext context) throws Exception {
		super.snapshotState(context);

		ListState<StreamElement> partitionableState =
			getOperatorStateBackend().getListState(new ListStateDescriptor<>(STATE_NAME, inStreamElementSerializer));
		partitionableState.clear();

		Collection<StreamElementQueueEntry<?>> values = queue.values();

		try {
			for (StreamElementQueueEntry<?> value : values) {
				partitionableState.add(value.getStreamElement());
			}
			// add the pending stream element queue entry if the stream element queue is currently full
			if (pendingStreamElementQueueEntry != null) {
				partitionableState.add(pendingStreamElementQueueEntry.getStreamElement());
			}
		} catch (Exception e) {
			partitionableState.clear();

			throw new Exception("Could not add stream element queue entries to operator state " +
				"backend of operator " + getOperatorName() + '.', e);
		}
	}

	@Override
	public void initializeState(StateInitializationContext context) throws Exception {
		super.initializeState(context);
		// recoveredStreamElements 存放在 operator state 中
		recoveredStreamElements = context
			.getOperatorStateStore()
			.getListState(new ListStateDescriptor<>(STATE_NAME, inStreamElementSerializer));

	}

	@Override
	public void close() throws Exception {
		try {
			assert(Thread.holdsLock(checkpointingLock));

			while (!queue.isEmpty()) {
				// wait for the emitter thread to output the remaining elements
				// for that he needs the checkpointing lock and thus we have to free it
				// 等待 emitter 线程 output 剩余的元素
				checkpointingLock.wait();
			}
		}
		finally {
			Exception exception = null;

			try {
				super.close();
			} catch (InterruptedException interrupted) {
				exception = interrupted;

				Thread.currentThread().interrupt();
			} catch (Exception e) {
				exception = e;
			}

			try {
				// terminate the emitter, the emitter thread and the executor
				// 停止 emitter 线程和 executor
				stopResources(true);
			} catch (InterruptedException interrupted) {
				exception = ExceptionUtils.firstOrSuppressed(interrupted, exception);

				Thread.currentThread().interrupt();
			} catch (Exception e) {
				exception = ExceptionUtils.firstOrSuppressed(e, exception);
			}

			if (exception != null) {
				LOG.warn("Errors occurred while closing the AsyncWaitOperator.", exception);
			}
		}
	}

	@Override
	public void dispose() throws Exception {
		Exception exception = null;

		try {
			super.dispose();
		} catch (InterruptedException interrupted) {
			exception = interrupted;

			Thread.currentThread().interrupt();
		} catch (Exception e) {
			exception = e;
		}

		try {
			stopResources(false);
		} catch (InterruptedException interrupted) {
			exception = ExceptionUtils.firstOrSuppressed(interrupted, exception);

			Thread.currentThread().interrupt();
		} catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(e, exception);
		}

		if (exception != null) {
			throw exception;
		}
	}

	/**
	 * Close the operator's resources. They include the emitter thread and the executor to run
	 * the queue's complete operation.
	 *
	 * @param waitForShutdown is true if the method should wait for the resources to be freed;
	 *                           otherwise false.
	 * @throws InterruptedException if current thread has been interrupted
	 */
	/**
	 * 关闭操作符的资源
	 * 包括 emitter 线程以及执行 queue 完成操作的 executor
	 */
	private void stopResources(boolean waitForShutdown) throws InterruptedException {
		emitter.stop();
		emitterThread.interrupt();

		executor.shutdown();

		if (waitForShutdown) {
			try {
				if (!executor.awaitTermination(365L, TimeUnit.DAYS)) {
					executor.shutdownNow();
				}
			} catch (InterruptedException e) {
				executor.shutdownNow();

				Thread.currentThread().interrupt();
			}

			/*
			 * FLINK-5638: If we have the checkpoint lock we might have to free it for a while so
			 * that the emitter thread can complete/react to the interrupt signal.
			 */
			if (Thread.holdsLock(checkpointingLock)) {
				while (emitterThread.isAlive()) {
					checkpointingLock.wait(100L);
				}
			}

			emitterThread.join();
		} else {
			executor.shutdownNow();
		}
	}

	/**
	 * Add the given stream element queue entry to the operator's stream element queue. This
	 * operation blocks until the element has been added.
	 *
	 * <p>For that it tries to put the element into the queue and if not successful then it waits on
	 * the checkpointing lock. The checkpointing lock is also used by the {@link Emitter} to output
	 * elements. The emitter is also responsible for notifying this method if the queue has capacity
	 * left again, by calling notifyAll on the checkpointing lock.
	 *
	 * @param streamElementQueueEntry to add to the operator's queue
	 * @param <T> Type of the stream element queue entry's result
	 * @throws InterruptedException if the current thread has been interrupted
	 */
	/**
	 * 将给定的 StreamElementQueueEntry 加入操作符的 StreamElementQueue
	 * 这个操作在元素被加入之前是阻塞的
	 */
	private <T> void addAsyncBufferEntry(StreamElementQueueEntry<T> streamElementQueueEntry) throws InterruptedException {
		assert(Thread.holdsLock(checkpointingLock));

		pendingStreamElementQueueEntry = streamElementQueueEntry;

		while (!queue.tryPut(streamElementQueueEntry)) {
			// we wait for the emitter to notify us if the queue has space left again
			// 我们等待 emitter 告诉我们队列中有空余位置
			// 这里用 put 方法感觉就可以。。put 方法里有 Reentrantlock 的 condition 控制
			checkpointingLock.wait();
		}

		pendingStreamElementQueueEntry = null;
	}

	@Override
	public void failOperator(Throwable throwable) {
		getContainingTask().getEnvironment().failExternally(throwable);
	}
}
