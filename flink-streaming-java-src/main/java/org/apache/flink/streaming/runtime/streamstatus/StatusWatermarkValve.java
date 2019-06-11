/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.streamstatus;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Preconditions;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@code StatusWatermarkValve} embodies the logic of how {@link Watermark} and {@link StreamStatus} are propagated to
 * downstream outputs, given a set of one or multiple input channels that continuously receive them. Usages of this
 * class need to define the number of input channels that the valve needs to handle, as well as provide a customized
 * implementation of {@link ValveOutputHandler}, which is called by the valve only when it determines a new watermark or
 * stream status can be propagated.
 */
/**
 * StatusWatermarkValve 体现了 Watermark 和 StreamStatus 如何传播到下游输出的逻辑
 * 此类的用法需要定义 value 需要处理的输入通道的数量，以及提供 ValveOutputHandler 的实现类
 * ValveOutputHandler 仅在确定新的水印或流状态可以传播时由 value 调用
 */
@Internal
public class StatusWatermarkValve {

	/**
	 * Usages of {@code StatusWatermarkValve} should implement a {@code ValveOutputHandler}
	 * to handle watermark and stream status outputs from the valve.
	 */
	public interface ValveOutputHandler {
		void handleWatermark(Watermark watermark);

		void handleStreamStatus(StreamStatus streamStatus);
	}

	private final ValveOutputHandler outputHandler;

	// ------------------------------------------------------------------------
	//	Runtime state for watermark & stream status output determination
	// ------------------------------------------------------------------------

	/**
	 * Array of current status of all input channels. Changes as watermarks & stream statuses are
	 * fed into the valve.
	 */
	/**
	 * 所有输入通道的当前状态数组
	 */
	private final InputChannelStatus[] channelStatuses;

	/** The last watermark emitted from the valve. */
	// value emit 的上一个 watermark
	private long lastOutputWatermark;

	/** The last stream status emitted from the valve. */
	// value emit 的上一个 StreamStatus
	private StreamStatus lastOutputStreamStatus;

	/**
	 * Returns a new {@code StatusWatermarkValve}.
	 *
	 * @param numInputChannels the number of input channels that this valve will need to handle
	 * @param outputHandler the customized output handler for the valve
	 */
	/**
	 * 构造函数，返回一个 StatusWatermarkValve 实例
	 * 初始化 InputChannelStatus 数组
	 */
	public StatusWatermarkValve(int numInputChannels, ValveOutputHandler outputHandler) {
		checkArgument(numInputChannels > 0);
		this.channelStatuses = new InputChannelStatus[numInputChannels];
		for (int i = 0; i < numInputChannels; i++) {
			channelStatuses[i] = new InputChannelStatus();
			channelStatuses[i].watermark = Long.MIN_VALUE;
			channelStatuses[i].streamStatus = StreamStatus.ACTIVE;
			channelStatuses[i].isWatermarkAligned = true;
		}

		this.outputHandler = checkNotNull(outputHandler);

		this.lastOutputWatermark = Long.MIN_VALUE;
		this.lastOutputStreamStatus = StreamStatus.ACTIVE;
	}

	/**
	 * Feed a {@link Watermark} into the valve. If the input triggers the valve to output a new Watermark,
	 * {@link ValveOutputHandler#handleWatermark(Watermark)} will be called to process the new Watermark.
	 *
	 * @param watermark the watermark to feed to the valve
	 * @param channelIndex the index of the channel that the fed watermark belongs to (index starting from 0)
	 */
	/**
	 * 将水印送入 Value，如果输入触发了 value 输出一个新的水印，调用 handleWatermark 来处理新的水印
	 */
	public void inputWatermark(Watermark watermark, int channelIndex) {
		// ignore the input watermark if its input channel, or all input channels are idle (i.e. overall the valve is idle).
		// 当全部的输入通道或者执行下标的输入通道空闲的时候，忽略输入的水印
		if (lastOutputStreamStatus.isActive() && channelStatuses[channelIndex].streamStatus.isActive()) {
			long watermarkMillis = watermark.getTimestamp();

			// if the input watermark's value is less than the last received watermark for its input channel, ignore it also.
			if (watermarkMillis > channelStatuses[channelIndex].watermark) {
				// 当水印大于当前通道的水印，更新
				channelStatuses[channelIndex].watermark = watermarkMillis;

				// previously unaligned input channels are now aligned if its watermark has caught up
				// 更新对齐状态
				if (!channelStatuses[channelIndex].isWatermarkAligned && watermarkMillis >= lastOutputWatermark) {
					channelStatuses[channelIndex].isWatermarkAligned = true;
				}

				// now, attempt to find a new min watermark across all aligned channels
				// 现在，尝试在所有对齐的通道上找到新的最小水印
				findAndOutputNewMinWatermarkAcrossAlignedChannels();
			}
		}
	}

	/**
	 * Feed a {@link StreamStatus} into the valve. This may trigger the valve to output either a new Stream Status,
	 * for which {@link ValveOutputHandler#handleStreamStatus(StreamStatus)} will be called, or a new Watermark,
	 * for which {@link ValveOutputHandler#handleWatermark(Watermark)} will be called.
	 *
	 * @param streamStatus the stream status to feed to the valve
	 * @param channelIndex the index of the channel that the fed stream status belongs to (index starting from 0)
	 */
	/**
	 * 将 StreamStatus 传入 value，有可能触发新的 StreamStatus 的生成或者新的 Watermark 的生成
	 */
	public void inputStreamStatus(StreamStatus streamStatus, int channelIndex) {
		// only account for stream status inputs that will result in a status change for the input channel
		// 仅考虑流状态输入，这将导致输入通道的状态更改
		if (streamStatus.isIdle() && channelStatuses[channelIndex].streamStatus.isActive()) {
			// handle active -> idle toggle for the input channel
			// 将当前 channel 的状态从 active 变为 idle
			channelStatuses[channelIndex].streamStatus = StreamStatus.IDLE;

			// the channel is now idle, therefore not aligned
			// 当前 channel 空闲了，因此不对齐了
			channelStatuses[channelIndex].isWatermarkAligned = false;

			// if all input channels of the valve are now idle, we need to output an idle stream
			// status from the valve (this also marks the valve as idle)
			// 如果所有的输入通道都空闲了，我们需要输出一个 idle 流状态
			if (!InputChannelStatus.hasActiveChannels(channelStatuses)) {

				// now that all input channels are idle and no channels will continue to advance its watermark,
				// we should "flush" all watermarks across all channels; effectively, this means emitting
				// the max watermark across all channels as the new watermark. Also, since we already try to advance
				// the min watermark as channels individually become IDLE, here we only need to perform the flush
				// if the watermark of the last active channel that just became idle is the current min watermark.
				// 既然所有输入通道都是空闲的，没有通道可以继续更新其水印，我们应该“冲洗”所有通道上的所有水印
				// 实际上，这意味着在所有通道上发出最大水印作为新水印。此外，由于我们总是尝试更新最小水印（findAndOutputNewMinWatermarkAcrossAlignedChannels方法）
				// 因此只有刚刚变为空闲的最后一个活动通道的水印是当前最小水印，我们需要执行刷新
				if (channelStatuses[channelIndex].watermark == lastOutputWatermark) {
					findAndOutputMaxWatermarkAcrossAllChannels();
				}

				lastOutputStreamStatus = StreamStatus.IDLE;
				outputHandler.handleStreamStatus(lastOutputStreamStatus);
			} else if (channelStatuses[channelIndex].watermark == lastOutputWatermark) {
				// if the watermark of the channel that just became idle equals the last output
				// watermark (the previous overall min watermark), we may be able to find a new
				// min watermark from the remaining aligned channels
				// 如果刚刚变为空闲的信道的水印等于最后的输出水印（前一个整体最小水印）
				// 我们可能能够从剩余的对齐频道中找到新的最小水印
				findAndOutputNewMinWatermarkAcrossAlignedChannels();
			}
		} else if (streamStatus.isActive() && channelStatuses[channelIndex].streamStatus.isIdle()) {
			// handle idle -> active toggle for the input channel
			// 将当前 channel 的状态从 idle 变为 active
			channelStatuses[channelIndex].streamStatus = StreamStatus.ACTIVE;

			// if the last watermark of the input channel, before it was marked idle, is still larger than
			// the overall last output watermark of the valve, then we can set the channel to be aligned already.
			// 重新对齐
			if (channelStatuses[channelIndex].watermark >= lastOutputWatermark) {
				channelStatuses[channelIndex].isWatermarkAligned = true;
			}

			// if the valve was previously marked to be idle, mark it as active and output an active stream
			// status because at least one of the input channels is now active
			// 有一个通道活跃了，整体也活跃了
			if (lastOutputStreamStatus.isIdle()) {
				lastOutputStreamStatus = StreamStatus.ACTIVE;
				outputHandler.handleStreamStatus(lastOutputStreamStatus);
			}
		}
	}

	// 尝试在所有对齐的通道上找到新的最小水印
	private void findAndOutputNewMinWatermarkAcrossAlignedChannels() {
		long newMinWatermark = Long.MAX_VALUE;
		boolean hasAlignedChannels = false;

		// determine new overall watermark by considering only watermark-aligned channels across all channels
		for (InputChannelStatus channelStatus : channelStatuses) {
			if (channelStatus.isWatermarkAligned) {
				hasAlignedChannels = true;
				newMinWatermark = Math.min(channelStatus.watermark, newMinWatermark);
			}
		}

		// we acknowledge and output the new overall watermark if it really is aggregated
		// from some remaining aligned channel, and is also larger than the last output watermark
		// 更新全局的 watermark
		if (hasAlignedChannels && newMinWatermark > lastOutputWatermark) {
			lastOutputWatermark = newMinWatermark;
			outputHandler.handleWatermark(new Watermark(lastOutputWatermark));
		}
	}

	/**
	 * 从所有 channel 中找出 watermark 最大的
	 */
	private void findAndOutputMaxWatermarkAcrossAllChannels() {
		long maxWatermark = Long.MIN_VALUE;

		for (InputChannelStatus channelStatus : channelStatuses) {
			maxWatermark = Math.max(channelStatus.watermark, maxWatermark);
		}

		if (maxWatermark > lastOutputWatermark) {
			lastOutputWatermark = maxWatermark;
			outputHandler.handleWatermark(new Watermark(lastOutputWatermark));
		}
	}

	/**
	 * An {@code InputChannelStatus} keeps track of an input channel's last watermark, stream
	 * status, and whether or not the channel's current watermark is aligned with the overall
	 * watermark output from the valve.
	 *
	 * <p>There are 2 situations where a channel's watermark is not considered aligned:
	 * <ul>
	 *   <li>the current stream status of the channel is idle
	 *   <li>the stream status has resumed to be active, but the watermark of the channel hasn't
	 *   caught up to the last output watermark from the valve yet.
	 * </ul>
	 */
	/**
	 * InputChannelStatus 跟踪输入通道的最后一个水印，流状态，以及通道的当前水印是否与 Value 的整体水印输出对齐
	 * 有两种情况下，通道的水印不被视为对齐
	 * 1. 通道的当前流状态是空闲的
	 * 2. 流状态已恢复为活动状态，但通道的水印尚未赶上来自 value 的最后输出水印
	 */
	@VisibleForTesting
	protected static class InputChannelStatus {
		protected long watermark;
		protected StreamStatus streamStatus;
		protected boolean isWatermarkAligned;

		/**
		 * Utility to check if at least one channel in a given array of input channels is active.
		 */
		/**
		 * 检查是否至少有一个通道是 active 的
		 */
		private static boolean hasActiveChannels(InputChannelStatus[] channelStatuses) {
			for (InputChannelStatus status : channelStatuses) {
				if (status.streamStatus.isActive()) {
					return true;
				}
			}
			return false;
		}
	}

	@VisibleForTesting
	// 获取指定 channelIndex 的通道状态
	protected InputChannelStatus getInputChannelStatus(int channelIndex) {
		Preconditions.checkArgument(
			channelIndex >= 0 && channelIndex < channelStatuses.length,
			"Invalid channel index. Number of input channels: " + channelStatuses.length);

		return channelStatuses[channelIndex];
	}
}
