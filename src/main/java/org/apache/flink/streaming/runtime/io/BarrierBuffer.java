/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.decline.AlignmentLimitExceededException;
import org.apache.flink.runtime.checkpoint.decline.CheckpointDeclineException;
import org.apache.flink.runtime.checkpoint.decline.CheckpointDeclineOnCancellationBarrierException;
import org.apache.flink.runtime.checkpoint.decline.CheckpointDeclineSubsumedException;
import org.apache.flink.runtime.checkpoint.decline.InputEndOfStreamException;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The barrier buffer is {@link CheckpointBarrierHandler} that blocks inputs with barriers until
 * all inputs have received the barrier for a given checkpoint.
 *
 * <p>To avoid back-pressuring the input streams (which may cause distributed deadlocks), the
 * BarrierBuffer continues receiving buffers from the blocked channels and stores them internally until
 * the blocks are released.
 */
/**
 * BarrierBuffer 是 CheckpointBarrierHandler
 * 它阻止带有障碍的输入，直到所有输入都接收到给定检查点的屏障
 * 
 * 为了避免对输入流进行反压（这可能导致分布式死锁）
 * BarrierBuffer 持续从阻塞的通道接收缓冲区并在内部存储它们直到块被释放
 * 
 * BarrierBuffer 与 BarrierTracker 完全不同
 * BarrierTracker 同时保存多个 checkpointId 的计数
 * BarrierBuffer 只保存一个 checkpointId 的阻塞通道
 */
@Internal
public class BarrierBuffer implements CheckpointBarrierHandler {

	private static final Logger LOG = LoggerFactory.getLogger(BarrierBuffer.class);

	/** The gate that the buffer draws its input from. */
	// input gate，用于绘制缓冲区和事件
	private final InputGate inputGate;

	/** Flags that indicate whether a channel is currently blocked/buffered. */
	// 指示通道当前是否被阻塞/缓冲的标志
	private final boolean[] blockedChannels;

	/** The total number of channels that this buffer handles data from. */
	// channel 的数量，一旦检查点收到了这么多 barriers，检查点被认为完成
	private final int totalNumberOfInputChannels;

	/** To utility to write blocked data to a file channel. */
	// 用于缓存 Buffer 和 Event
	private final BufferBlocker bufferBlocker;

	/**
	 * The pending blocked buffer/event sequences. Must be consumed before requesting further data
	 * from the input gate.
	 */
	/**
	 * pending 的缓冲区/事件序列，必须在从 input gate 请求更多数据之前消费
	 */
	private final ArrayDeque<BufferOrEventSequence> queuedBuffered;

	/**
	 * The maximum number of bytes that may be buffered before an alignment is broken. -1 means
	 * unlimited.
	 */
	/**
	 * 在对齐被破坏之前可以缓冲的最大字节数，-1表示无限制
	 */
	private final long maxBufferedBytes;

	/**
	 * The sequence of buffers/events that has been unblocked and must now be consumed before
	 * requesting further data from the input gate.
	 */
	/**
	 * 解除阻塞的 buffers/events 队列，在从 input gate 请求更多数据之前
	 * 必须消费本队列
	 */
	private BufferOrEventSequence currentBuffered;

	/** Handler that receives the checkpoint notifications. */
	// 接收检查点通知的处理程序
	private AbstractInvokable toNotifyOnCheckpoint;

	/** The ID of the checkpoint for which we expect barriers. */
	// 我们期望障碍的检查点的ID
	private long currentCheckpointId = -1L;

	/**
	 * The number of received barriers (= number of blocked/buffered channels) IMPORTANT: A canceled
	 * checkpoint must always have 0 barriers.
	 */
	/**
	 * 已接收障碍的数量，需要注意的是，一个取消的检查点必须总是拥有 0 个障碍
	 */
	private int numBarriersReceived;

	/** The number of already closed channels. */
	// 当前已经关闭的 channel 的数目
	private int numClosedChannels;

	/** The number of bytes in the queued spilled sequences. */
	// 排队的溢出序列中的字节数
	private long numQueuedBytes;

	/** The timestamp as in {@link System#nanoTime()} at which the last alignment started. */
	// 最近一次对齐开始的时间
	private long startOfAlignmentTimestamp;

	/** The time (in nanoseconds) that the latest alignment took. */
	// 最近的对齐花费的时间
	private long latestAlignmentDurationNanos;

	/** Flag to indicate whether we have drawn all available input. */
	// 标记以指示我们是否已绘制所有可用输入
	private boolean endOfStream;

	/**
	 * Creates a new checkpoint stream aligner.
	 *
	 * <p>There is no limit to how much data may be buffered during an alignment.
	 *
	 * @param inputGate The input gate to draw the buffers and events from.
	 * @param bufferBlocker The buffer blocker to hold the buffers and events for channels with barrier.
	 *
	 * @throws IOException Thrown, when the spilling to temp files cannot be initialized.
	 */
	/**
	 * 创建一个新的检查点流对齐器，在对齐期间可以缓冲多少数据没有限制
	 */
	public BarrierBuffer(InputGate inputGate, BufferBlocker bufferBlocker) throws IOException {
		this (inputGate, bufferBlocker, -1);
	}

	/**
	 * Creates a new checkpoint stream aligner.
	 *
	 * <p>The aligner will allow only alignments that buffer up to the given number of bytes.
	 * When that number is exceeded, it will stop the alignment and notify the task that the
	 * checkpoint has been cancelled.
	 *
	 * @param inputGate The input gate to draw the buffers and events from.
	 * @param bufferBlocker The buffer blocker to hold the buffers and events for channels with barrier.
	 * @param maxBufferedBytes The maximum bytes to be buffered before the checkpoint aborts.
	 *
	 * @throws IOException Thrown, when the spilling to temp files cannot be initialized.
	 */
	/**
	 * 创建一个新的检查点流对齐器
	 * 对齐器将仅允许缓冲达到给定字节数的对齐。超过该数量时，它将停止对齐并通知任务已取消检查点
	 */
	public BarrierBuffer(InputGate inputGate, BufferBlocker bufferBlocker, long maxBufferedBytes)
			throws IOException {
		checkArgument(maxBufferedBytes == -1 || maxBufferedBytes > 0);

		this.inputGate = inputGate;
		this.maxBufferedBytes = maxBufferedBytes;
		this.totalNumberOfInputChannels = inputGate.getNumberOfInputChannels();
		this.blockedChannels = new boolean[this.totalNumberOfInputChannels];

		this.bufferBlocker = checkNotNull(bufferBlocker);
		this.queuedBuffered = new ArrayDeque<BufferOrEventSequence>();
	}

	// ------------------------------------------------------------------------
	//  Buffer and barrier handling
	// ------------------------------------------------------------------------

	@Override
	public BufferOrEvent getNextNonBlocked() throws Exception {
		while (true) {
			// process buffered BufferOrEvents before grabbing new ones
			// 在获取新的缓冲序列之前，处理当前的
			Optional<BufferOrEvent> next;
			// 如果 currentBuffered 为空，则可以从 input gate 去请求新的数据
			if (currentBuffered == null) {
				next = inputGate.getNextBufferOrEvent();
			}
			// 否则，需要先处理 currentBuffered
			else {
				next = Optional.ofNullable(currentBuffered.getNext());
				// next 为 null 的话，完成本 BufferOrEventSequence
				if (!next.isPresent()) {
					completeBufferedSequence();
					return getNextNonBlocked();
				}
			}
			
			// input gate 输入为 null
			if (!next.isPresent()) {
				if (!endOfStream) {
					// end of input stream. stream continues with the buffered data
					// 输入流结束，流继续缓冲数据
					endOfStream = true;
					releaseBlocksAndResetBarriers();
					return getNextNonBlocked();
				}
				else {
					// final end of both input and buffered data
					// 输入和缓冲数据的最终结束
					return null;
				}
			}

			BufferOrEvent bufferOrEvent = next.get();
			if (isBlocked(bufferOrEvent.getChannelIndex())) {
				// if the channel is blocked, we just store the BufferOrEvent
				// 如果当前 channel 被阻塞了，我们缓存 BufferOrEvent
				bufferBlocker.add(bufferOrEvent);
				checkSizeLimit();
			}
			else if (bufferOrEvent.isBuffer()) {
				return bufferOrEvent;
			}
			else if (bufferOrEvent.getEvent().getClass() == CheckpointBarrier.class) {
				if (!endOfStream) {
					// process barriers only if there is a chance of the checkpoint completing
					// 只有在检查点有可能完成时才会处理障碍
					processBarrier((CheckpointBarrier) bufferOrEvent.getEvent(), bufferOrEvent.getChannelIndex());
				}
			}
			else if (bufferOrEvent.getEvent().getClass() == CancelCheckpointMarker.class) {
				processCancellationBarrier((CancelCheckpointMarker) bufferOrEvent.getEvent());
			}
			else {
				// This event marks a subpartition as fully consumed
				// EndOfPartitionEvent 标志着一个子分区完全被消费完毕
				if (bufferOrEvent.getEvent().getClass() == EndOfPartitionEvent.class) {
					processEndOfPartition();
				}
				return bufferOrEvent;
			}
		}
	}

	// 完成 currentBuffered 这个队列
	private void completeBufferedSequence() throws IOException {
		LOG.debug("{}: Finished feeding back buffered data.", inputGate.getOwningTaskName());

		currentBuffered.cleanup();  // 先释放资源
		currentBuffered = queuedBuffered.pollFirst();  // 然后从队列中取出一个新的 BufferOrEventSequence
		if (currentBuffered != null) {
			currentBuffered.open();
			numQueuedBytes -= currentBuffered.size();  // 减去排队的数量
		}
	}

	/**
	 * 处理 barrier
	 */
	private void processBarrier(CheckpointBarrier receivedBarrier, int channelIndex) throws Exception {
		final long barrierId = receivedBarrier.getId();

		// fast path for single channel cases
		// 单输入 channel 的快捷通道
		if (totalNumberOfInputChannels == 1) {
			if (barrierId > currentCheckpointId) {
				// new checkpoint
				currentCheckpointId = barrierId;
				notifyCheckpoint(receivedBarrier);
			}
			return;
		}

		// -- general code path for multiple input channels --
		// 多输入 channel 的通用代码
		if (numBarriersReceived > 0) {
			// this is only true if some alignment is already progress and was not canceled
			// 只有在某些对齐已经进行并且未取消时才会出现这种情况
			if (barrierId == currentCheckpointId) {
				// regular case
				// 常规 case
				onBarrier(channelIndex);
			}
			else if (barrierId > currentCheckpointId) {
				// we did not complete the current checkpoint, another started before
				// 我们没有在另一个检查点开始之前，完成当前的检查点
				LOG.warn("{}: Received checkpoint barrier for checkpoint {} before completing current checkpoint {}. " +
						"Skipping current checkpoint.",
					inputGate.getOwningTaskName(),
					barrierId,
					currentCheckpointId);

				// let the task know we are not completing this
				// 让任务知道我们没有完成当前检查点
				notifyAbort(currentCheckpointId, new CheckpointDeclineSubsumedException(barrierId));

				// abort the current checkpoint
				// 停止当前的检查点，这里要开始一个新的检查点，所以 releaseBlocksAndResetBarriers 新
				// 生成一个 BufferOrEventSequence，将老的返回队列，因为这样更有机会能访问到新检查点的 barrier
				releaseBlocksAndResetBarriers();

				// begin a the new checkpoint
				// 开始一个新的检查点
				beginNewAlignment(barrierId, channelIndex);
			}
			else {
				// ignore trailing barrier from an earlier checkpoint (obsolete now)
				// 忽略早期检查点的尾随障碍（现在已过时）
				return;
			}
		}
		else if (barrierId > currentCheckpointId) {
			// first barrier of a new checkpoint
			// 新的检查点的第一个 barrier
			beginNewAlignment(barrierId, channelIndex);
		}
		else {
			// either the current checkpoint was canceled (numBarriers == 0) or
			// this barrier is from an old subsumed checkpoint
			// 要么当前检查点被取消了（numBarriers == 0）
			// 要么此屏障来自旧的包含检查点
			return;
		}

		// check if we have all barriers - since canceled checkpoints always have zero barriers
		// this can only happen on a non canceled checkpoint
		// 检查我们是否有所有的障碍 - 因为取消的检查点始终没有障碍，这只能在未取消的检查点上发生
		if (numBarriersReceived + numClosedChannels == totalNumberOfInputChannels) {
			// actually trigger checkpoint
			// 触发检查点
			if (LOG.isDebugEnabled()) {
				LOG.debug("{}: Received all barriers, triggering checkpoint {} at {}.",
					inputGate.getOwningTaskName(),
					receivedBarrier.getId(),
					receivedBarrier.getTimestamp());
			}

			releaseBlocksAndResetBarriers();
			notifyCheckpoint(receivedBarrier);
		}
	}

	// 处理 barrier 的取消
	private void processCancellationBarrier(CancelCheckpointMarker cancelBarrier) throws Exception {
		final long barrierId = cancelBarrier.getCheckpointId();

		// fast path for single channel cases
		// 依旧是单输入 channel 的快捷通道
		if (totalNumberOfInputChannels == 1) {
			if (barrierId > currentCheckpointId) {
				// new checkpoint
				currentCheckpointId = barrierId;
				notifyAbortOnCancellationBarrier(barrierId);
			}
			return;
		}

		// -- general code path for multiple input channels --
		// 多输入 channel 的通用代码
		if (numBarriersReceived > 0) {
			// this is only true if some alignment is in progress and nothing was canceled
			// 只有在某些对齐正在进行且没有取消任何内容时才会出现这种情况
			if (barrierId == currentCheckpointId) {
				// cancel this alignment
				if (LOG.isDebugEnabled()) {
					LOG.debug("{}: Checkpoint {} canceled, aborting alignment.",
						inputGate.getOwningTaskName(),
						barrierId);
				}

				releaseBlocksAndResetBarriers();
				notifyAbortOnCancellationBarrier(barrierId);
			}
			else if (barrierId > currentCheckpointId) {
				// we canceled the next which also cancels the current
				// 我们取消了之后的检查点，自然也取消当前的
				LOG.warn("{}: Received cancellation barrier for checkpoint {} before completing current checkpoint {}. " +
						"Skipping current checkpoint.",
					inputGate.getOwningTaskName(),
					barrierId,
					currentCheckpointId);

				// this stops the current alignment
				// 这会停止当前对齐
				releaseBlocksAndResetBarriers();

				// the next checkpoint starts as canceled
				currentCheckpointId = barrierId;
				startOfAlignmentTimestamp = 0L;
				latestAlignmentDurationNanos = 0L;

				notifyAbort(currentCheckpointId, new CheckpointDeclineSubsumedException(barrierId));

				notifyAbortOnCancellationBarrier(barrierId);
			}

			// else: ignore trailing (cancellation) barrier from an earlier checkpoint (obsolete now)

		}
		else if (barrierId > currentCheckpointId) {
			// first barrier of a new checkpoint is directly a cancellation
			// 新检查点的第一道屏障直接取消

			// by setting the currentCheckpointId to this checkpoint while keeping the numBarriers
			// at zero means that no checkpoint barrier can start a new alignment
			currentCheckpointId = barrierId;

			startOfAlignmentTimestamp = 0L;
			latestAlignmentDurationNanos = 0L;

			if (LOG.isDebugEnabled()) {
				LOG.debug("{}: Checkpoint {} canceled, skipping alignment.",
					inputGate.getOwningTaskName(),
					barrierId);
			}

			notifyAbortOnCancellationBarrier(barrierId);
		}

		// else: trailing barrier from either
		//   - a previous (subsumed) checkpoint
		//   - the current checkpoint if it was already canceled
	}

	// 分区消费完毕
	private void processEndOfPartition() throws Exception {
		numClosedChannels++;

		if (numBarriersReceived > 0) {
			// let the task know we skip a checkpoint
			// 让任务知道我们跳过了检查点
			notifyAbort(currentCheckpointId, new InputEndOfStreamException());

			// no chance to complete this checkpoint
			// 没机会完成这个检查点
			releaseBlocksAndResetBarriers();
		}
	}

	/**
	 * 通知检查点完成
	 */
	private void notifyCheckpoint(CheckpointBarrier checkpointBarrier) throws Exception {
		if (toNotifyOnCheckpoint != null) {
			CheckpointMetaData checkpointMetaData =
					new CheckpointMetaData(checkpointBarrier.getId(), checkpointBarrier.getTimestamp());

			long bytesBuffered = currentBuffered != null ? currentBuffered.size() : 0L;

			CheckpointMetrics checkpointMetrics = new CheckpointMetrics()
					.setBytesBufferedInAlignment(bytesBuffered)
					.setAlignmentDurationNanos(latestAlignmentDurationNanos);

			toNotifyOnCheckpoint.triggerCheckpointOnBarrier(
				checkpointMetaData,
				checkpointBarrier.getCheckpointOptions(),
				checkpointMetrics);
		}
	}

	// 因为收到了取消检查点的 event，取消 id 为 checkpointId 的检查点
	private void notifyAbortOnCancellationBarrier(long checkpointId) throws Exception {
		notifyAbort(checkpointId, new CheckpointDeclineOnCancellationBarrierException());
	}

	/**
	 * 通知检查点取消
	 */
	private void notifyAbort(long checkpointId, CheckpointDeclineException cause) throws Exception {
		if (toNotifyOnCheckpoint != null) {
			toNotifyOnCheckpoint.abortCheckpointOnBarrier(checkpointId, cause);
		}
	}

	/**
	 * 检查当前缓存的总字节数是否超过了限制
	 */
	private void checkSizeLimit() throws Exception {
		// numQueuedBytes 是所有 BufferOrEventSequence 缓存的字节数
		// bufferBlocker.getBytesBlocked() 返回当前 bufferBlocker 中还没 roll over 的字节数
		if (maxBufferedBytes > 0 && (numQueuedBytes + bufferBlocker.getBytesBlocked()) > maxBufferedBytes) {
			// exceeded our limit - abort this checkpoint
			// 超出我们的限制 - 中止此检查点
			LOG.info("{}: Checkpoint {} aborted because alignment volume limit ({} bytes) exceeded.",
				inputGate.getOwningTaskName(),
				currentCheckpointId,
				maxBufferedBytes);

			releaseBlocksAndResetBarriers();
			// 超出最大缓存限制，取消检查点
			notifyAbort(currentCheckpointId, new AlignmentLimitExceededException(maxBufferedBytes));
		}
	}

	// 注册 StreamTask
	@Override
	public void registerCheckpointEventHandler(AbstractInvokable toNotifyOnCheckpoint) {
		if (this.toNotifyOnCheckpoint == null) {
			this.toNotifyOnCheckpoint = toNotifyOnCheckpoint;
		}
		else {
			throw new IllegalStateException("BarrierBuffer already has a registered checkpoint notifyee");
		}
	}

	@Override
	public boolean isEmpty() {
		return currentBuffered == null;
	}

	/**
	 * 释放所有申请的资源
	 */
	@Override
	public void cleanup() throws IOException {
		bufferBlocker.close();
		if (currentBuffered != null) {
			currentBuffered.cleanup();
		}
		for (BufferOrEventSequence seq : queuedBuffered) {
			seq.cleanup();
		}
		queuedBuffered.clear();
		numQueuedBytes = 0L;
	}

	/**
	 * 开始一个新的检查点
	 * 执行本方法之前执行过 releaseBlocksAndResetBarriers 方法
	 * blockedChannels 重置为全 false
	 */
	private void beginNewAlignment(long checkpointId, int channelIndex) throws IOException {
		currentCheckpointId = checkpointId;
		onBarrier(channelIndex);

		startOfAlignmentTimestamp = System.nanoTime();

		if (LOG.isDebugEnabled()) {
			LOG.debug("{}: Starting stream alignment for checkpoint {}.",
				inputGate.getOwningTaskName(),
				checkpointId);
		}
	}

	/**
	 * Checks whether the channel with the given index is blocked.
	 *
	 * @param channelIndex The channel index to check.
	 * @return True if the channel is blocked, false if not.
	 */
	/**
	 * 检查具有给定索引的通道是否被阻塞
	 */
	private boolean isBlocked(int channelIndex) {
		return blockedChannels[channelIndex];
	}

	/**
	 * Blocks the given channel index, from which a barrier has been received.
	 *
	 * @param channelIndex The channel index to block.
	 */
	/**
	 * 阻塞给定索引的通道
	 */
	private void onBarrier(int channelIndex) throws IOException {
		if (!blockedChannels[channelIndex]) {
			blockedChannels[channelIndex] = true;

			numBarriersReceived++;

			if (LOG.isDebugEnabled()) {
				LOG.debug("{}: Received barrier from channel {}.",
					inputGate.getOwningTaskName(),
					channelIndex);
			}
		}
		else {
			throw new IOException("Stream corrupt: Repeated barrier for same checkpoint on input " + channelIndex);
		}
	}

	/**
	 * Releases the blocks on all channels and resets the barrier count.
	 * Makes sure the just written data is the next to be consumed.
	 */
	/**
	 * 释放所有通道上的块并重置屏障计数
	 * 确保刚刚写入的数据是下一个要消耗的数据
	 */
	private void releaseBlocksAndResetBarriers() throws IOException {
		LOG.debug("{}: End of stream alignment, feeding buffered data back.",
			inputGate.getOwningTaskName());

		for (int i = 0; i < blockedChannels.length; i++) {
			blockedChannels[i] = false;
		}

		if (currentBuffered == null) {
			// common case: no more buffered data
			// 常见情况：没有更多缓冲数据
			currentBuffered = bufferBlocker.rollOverReusingResources();
			if (currentBuffered != null) {
				currentBuffered.open();
			}
		}
		else {
			// uncommon case: buffered data pending
			// push back the pending data, if we have any
			// 不常见的情况：缓冲数据 pending
			// 如果我们有 pending 数据的话，将其推回
			LOG.debug("{}: Checkpoint skipped via buffered data:" +
					"Pushing back current alignment buffers and feeding back new alignment data first.",
				inputGate.getOwningTaskName());

			// since we did not fully drain the previous sequence, we need to allocate a new buffer for this one
			// 因为我们没有完全消耗前面的序列，所以我们需要为这个序列分配一个新的缓冲区
			BufferOrEventSequence bufferedNow = bufferBlocker.rollOverWithoutReusingResources();
			if (bufferedNow != null) {
				bufferedNow.open();
				queuedBuffered.addFirst(currentBuffered);  // 这里是将当前的 currentBuffered 放入队列
				numQueuedBytes += currentBuffered.size();
				currentBuffered = bufferedNow;  // 然后设置 currentBuffered 为 bufferedNow
			}
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("{}: Size of buffered data: {} bytes",
				inputGate.getOwningTaskName(),
				currentBuffered == null ? 0L : currentBuffered.size());
		}

		// the next barrier that comes must assume it is the first
		// 下一个到来的障碍必须被假设是第一个
		numBarriersReceived = 0;

		if (startOfAlignmentTimestamp > 0) {
			latestAlignmentDurationNanos = System.nanoTime() - startOfAlignmentTimestamp;
			startOfAlignmentTimestamp = 0;
		}
	}

	// ------------------------------------------------------------------------
	//  Properties
	// ------------------------------------------------------------------------

	/**
	 * Gets the ID defining the current pending, or just completed, checkpoint.
	 *
	 * @return The ID of the pending of completed checkpoint.
	 */
	/**
	 * 获取定义当前挂起或刚刚完成的检查点的 ID
	 */
	public long getCurrentCheckpointId() {
		return this.currentCheckpointId;
	}

	@Override
	public long getAlignmentDurationNanos() {
		long start = this.startOfAlignmentTimestamp;
		if (start <= 0) {
			return latestAlignmentDurationNanos;
		} else {
			return System.nanoTime() - start;
		}
	}

	// ------------------------------------------------------------------------
	// Utilities
	// ------------------------------------------------------------------------

	@Override
	public String toString() {
		return String.format("%s: last checkpoint: %d, current barriers: %d, closed channels: %d",
			inputGate.getOwningTaskName(),
			currentCheckpointId,
			numBarriersReceived,
			numClosedChannels);
	}
}
