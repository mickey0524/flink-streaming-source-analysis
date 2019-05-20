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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.decline.CheckpointDeclineOnCancellationBarrierException;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Optional;

/**
 * The BarrierTracker keeps track of what checkpoint barriers have been received from
 * which input channels. Once it has observed all checkpoint barriers for a checkpoint ID,
 * it notifies its listener of a completed checkpoint.
 *
 * <p>Unlike the {@link BarrierBuffer}, the BarrierTracker does not block the input
 * channels that have sent barriers, so it cannot be used to gain "exactly-once" processing
 * guarantees. It can, however, be used to gain "at least once" processing guarantees.
 *
 * <p>NOTE: This implementation strictly assumes that newer checkpoints have higher checkpoint IDs.
 */
/**
 * BarrierTracker 跟踪从哪些输入通道接收到的检查点障碍
 * 一旦它观察到检查点 ID 的所有检查点障碍，它就会通知其监听器检查点已完成
 * 
 * 与 BarrierBuffer 不同，BarrierTracker 不会阻止已发送障碍的输入通道
 * 因此它不能用于获得 "完全一次" 处理保证
 * 但是，它可用于获得 "至少一次" 处理保证
 */
@Internal
public class BarrierTracker implements CheckpointBarrierHandler {

	private static final Logger LOG = LoggerFactory.getLogger(BarrierTracker.class);

	/**
	 * The tracker tracks a maximum number of checkpoints, for which some, but not all barriers
	 * have yet arrived.
	 */
	/**
	 * 跟踪器跟踪最大数量的检查点，其中有一些，但不是全部的障碍没有到达
	 */
	private static final int MAX_CHECKPOINTS_TO_TRACK = 50;

	// ------------------------------------------------------------------------

	/** The input gate, to draw the buffers and events from. */
	// input gate，用于绘制缓冲区和事件
	private final InputGate inputGate;

	/**
	 * The number of channels. Once that many barriers have been received for a checkpoint, the
	 * checkpoint is considered complete.
	 */
	// channel 的数量，一旦检查点收到了这么多 barriers，检查点被认为完成
	private final int totalNumberOfInputChannels;

	/**
	 * All checkpoints for which some (but not all) barriers have been received, and that are not
	 * yet known to be subsumed by newer checkpoints.
	 */
	/**
	 * 已收到一些（但不是全部）障碍的所有检查点，以及目前不知道是否还有更新的检查点
	 */
	private final ArrayDeque<CheckpointBarrierCount> pendingCheckpoints;

	/** The listener to be notified on complete checkpoints. */
	// 检查点完成的时候触发的监听器
	private AbstractInvokable toNotifyOnCheckpoint;

	/** The highest checkpoint ID encountered so far. */
	// 到目前位置遇到的最大检查点 ID
	private long latestPendingCheckpointID = -1;

	// ------------------------------------------------------------------------

	// 新建一个跟踪器
	public BarrierTracker(InputGate inputGate) {
		this.inputGate = inputGate;
		this.totalNumberOfInputChannels = inputGate.getNumberOfInputChannels();
		this.pendingCheckpoints = new ArrayDeque<>();
	}

	// 这是一个 blocking 的函数
	@Override
	public BufferOrEvent getNextNonBlocked() throws Exception {
		while (true) {
			Optional<BufferOrEvent> next = inputGate.getNextBufferOrEvent();
			if (!next.isPresent()) {
				// buffer or input exhausted
				// 缓存或输入耗尽
				return null;
			}

			BufferOrEvent bufferOrEvent = next.get();
			// 如果是 buffer 的话
			if (bufferOrEvent.isBuffer()) {
				return bufferOrEvent;
			}
			// 收到了检查点 barrier
			else if (bufferOrEvent.getEvent().getClass() == CheckpointBarrier.class) {
				processBarrier((CheckpointBarrier) bufferOrEvent.getEvent(), bufferOrEvent.getChannelIndex());
			}
			// 收到了取消检查点
			else if (bufferOrEvent.getEvent().getClass() == CancelCheckpointMarker.class) {
				processCheckpointAbortBarrier((CancelCheckpointMarker) bufferOrEvent.getEvent(), bufferOrEvent.getChannelIndex());
			}
			else {
				// some other event
				// 一些其他的 event
				return bufferOrEvent;
			}
		}
	}

	// 注册检查点时间处理器，其实就是设置 StreamTask
	@Override
	public void registerCheckpointEventHandler(AbstractInvokable toNotifyOnCheckpoint) {
		if (this.toNotifyOnCheckpoint == null) {
			this.toNotifyOnCheckpoint = toNotifyOnCheckpoint;
		}
		else {
			throw new IllegalStateException("BarrierTracker already has a registered checkpoint notifyee");
		}
	}

	@Override
	public void cleanup() {
		pendingCheckpoints.clear();
	}

	@Override
	public boolean isEmpty() {
		return pendingCheckpoints.isEmpty();
	}

	@Override
	public long getAlignmentDurationNanos() {
		// this one does not do alignment at all
		return 0L;
	}

	private void processBarrier(CheckpointBarrier receivedBarrier, int channelIndex) throws Exception {
		// 获取检查点 ID
		final long barrierId = receivedBarrier.getId();

		// fast path for single channel trackers
		// 单通道跟踪器的快速路径，只有一个通道的话，接到一个 Barrier，就说明检查点完成
		if (totalNumberOfInputChannels == 1) {
			notifyCheckpoint(barrierId, receivedBarrier.getTimestamp(), receivedBarrier.getCheckpointOptions());
			return;
		}

		// general path for multiple input channels
		if (LOG.isDebugEnabled()) {
			LOG.debug("Received barrier for checkpoint {} from channel {}", barrierId, channelIndex);
		}

		// find the checkpoint barrier in the queue of pending barriers
		CheckpointBarrierCount cbc = null;
		int pos = 0;

		for (CheckpointBarrierCount next : pendingCheckpoints) {
			if (next.checkpointId == barrierId) {
				cbc = next;
				break;
			}
			pos++;
		}

		if (cbc != null) {
			// add one to the count to that barrier and check for completion
			int numBarriersNew = cbc.incrementBarrierCount();
			if (numBarriersNew == totalNumberOfInputChannels) {
				// checkpoint can be triggered (or is aborted and all barriers have been seen)
				// first, remove this checkpoint and all all prior pending
				// checkpoints (which are now subsumed)
				for (int i = 0; i <= pos; i++) {
					pendingCheckpoints.pollFirst();
				}

				// notify the listener
				if (!cbc.isAborted()) {
					if (LOG.isDebugEnabled()) {
						LOG.debug("Received all barriers for checkpoint {}", barrierId);
					}

					notifyCheckpoint(receivedBarrier.getId(), receivedBarrier.getTimestamp(), receivedBarrier.getCheckpointOptions());
				}
			}
		}
		else {
			// first barrier for that checkpoint ID
			// add it only if it is newer than the latest checkpoint.
			// if it is not newer than the latest checkpoint ID, then there cannot be a
			// successful checkpoint for that ID anyways
			if (barrierId > latestPendingCheckpointID) {
				latestPendingCheckpointID = barrierId;
				pendingCheckpoints.addLast(new CheckpointBarrierCount(barrierId));

				// make sure we do not track too many checkpoints
				if (pendingCheckpoints.size() > MAX_CHECKPOINTS_TO_TRACK) {
					pendingCheckpoints.pollFirst();
				}
			}
		}
	}

	private void processCheckpointAbortBarrier(CancelCheckpointMarker barrier, int channelIndex) throws Exception {
		final long checkpointId = barrier.getCheckpointId();

		if (LOG.isDebugEnabled()) {
			LOG.debug("Received cancellation barrier for checkpoint {} from channel {}", checkpointId, channelIndex);
		}

		// fast path for single channel trackers
		if (totalNumberOfInputChannels == 1) {
			notifyAbort(checkpointId);
			return;
		}

		// -- general path for multiple input channels --

		// find the checkpoint barrier in the queue of pending barriers
		// while doing this we "abort" all checkpoints before that one
		CheckpointBarrierCount cbc;
		while ((cbc = pendingCheckpoints.peekFirst()) != null && cbc.checkpointId() < checkpointId) {
			pendingCheckpoints.removeFirst();

			if (cbc.markAborted()) {
				// abort the subsumed checkpoints if not already done
				notifyAbort(cbc.checkpointId());
			}
		}

		if (cbc != null && cbc.checkpointId() == checkpointId) {
			// make sure the checkpoint is remembered as aborted
			if (cbc.markAborted()) {
				// this was the first time the checkpoint was aborted - notify
				notifyAbort(checkpointId);
			}

			// we still count the barriers to be able to remove the entry once all barriers have been seen
			if (cbc.incrementBarrierCount() == totalNumberOfInputChannels) {
				// we can remove this entry
				pendingCheckpoints.removeFirst();
			}
		}
		else if (checkpointId > latestPendingCheckpointID) {
			notifyAbort(checkpointId);

			latestPendingCheckpointID = checkpointId;

			CheckpointBarrierCount abortedMarker = new CheckpointBarrierCount(checkpointId);
			abortedMarker.markAborted();
			pendingCheckpoints.addFirst(abortedMarker);

			// we have removed all other pending checkpoint barrier counts --> no need to check that
			// we don't exceed the maximum checkpoints to track
		} else {
			// trailing cancellation barrier which was already cancelled
		}
	}

	/**
	 * 通知检查点完成
	 */
	private void notifyCheckpoint(long checkpointId, long timestamp, CheckpointOptions checkpointOptions) throws Exception {
		if (toNotifyOnCheckpoint != null) {
			CheckpointMetaData checkpointMetaData = new CheckpointMetaData(checkpointId, timestamp);
			CheckpointMetrics checkpointMetrics = new CheckpointMetrics()
				.setBytesBufferedInAlignment(0L)
				.setAlignmentDurationNanos(0L);

			toNotifyOnCheckpoint.triggerCheckpointOnBarrier(checkpointMetaData, checkpointOptions, checkpointMetrics);
		}
	}

	/**
	 * 通知检查点取消
	 */
	private void notifyAbort(long checkpointId) throws Exception {
		if (toNotifyOnCheckpoint != null) {
			toNotifyOnCheckpoint.abortCheckpointOnBarrier(
					checkpointId, new CheckpointDeclineOnCancellationBarrierException());
		}
	}

	// ------------------------------------------------------------------------

	/**
	 * Simple class for a checkpoint ID with a barrier counter.
	 */
	/**
	 * checkpoint ID 用的简单的类
	 * 记录了收集到的 barrier 的数目
	 */
	private static final class CheckpointBarrierCount {

		private final long checkpointId;

		private int barrierCount;

		private boolean aborted;

		CheckpointBarrierCount(long checkpointId) {
			this.checkpointId = checkpointId;
			this.barrierCount = 1;
		}

		public long checkpointId() {
			return checkpointId;
		}

		public int incrementBarrierCount() {
			return ++barrierCount;
		}

		public boolean isAborted() {
			return aborted;
		}

		public boolean markAborted() {
			boolean firstAbort = !this.aborted;  // 是否是第一次 abort
			this.aborted = true;
			return firstAbort;
		}

		@Override
		public String toString() {
			return isAborted() ?
				String.format("checkpointID=%d - ABORTED", checkpointId) :
				String.format("checkpointID=%d, count=%d", checkpointId, barrierCount);
		}
	}
}
