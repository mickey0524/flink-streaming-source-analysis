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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusMaintainer;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.Preconditions;

import java.util.concurrent.ScheduledFuture;

/**
 * Source contexts for various stream time characteristics.
 */
/**
 * 不同流时间模式的 Source context
 */
public class StreamSourceContexts {

	/**
	 * Depending on the {@link TimeCharacteristic}, this method will return the adequate
	 * {@link org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext}. That is:
	 * <ul>
	 *     <li>{@link TimeCharacteristic#IngestionTime} = {@code AutomaticWatermarkContext}</li>
	 *     <li>{@link TimeCharacteristic#ProcessingTime} = {@code NonTimestampContext}</li>
	 *     <li>{@link TimeCharacteristic#EventTime} = {@code ManualWatermarkContext}</li>
	 * </ul>
	 * */
	/**
	 * 根据不同的 TimeCharacteristic 返回不同的 SourceContext
	 * IngestionTime => AutomaticWatermarkContext
	 * ProcessingTime => NonTimestampContext
	 * EventTime => ManualWatermarkContext
	 */
	public static <OUT> SourceFunction.SourceContext<OUT> getSourceContext(
			TimeCharacteristic timeCharacteristic,
			ProcessingTimeService processingTimeService,
			Object checkpointLock,
			StreamStatusMaintainer streamStatusMaintainer,
			Output<StreamRecord<OUT>> output,
			long watermarkInterval,
			long idleTimeout) {

		final SourceFunction.SourceContext<OUT> ctx;
		switch (timeCharacteristic) {
			case EventTime:
				ctx = new ManualWatermarkContext<>(
					output,
					processingTimeService,
					checkpointLock,
					streamStatusMaintainer,
					idleTimeout);

				break;
			case IngestionTime:
				ctx = new AutomaticWatermarkContext<>(
					output,
					watermarkInterval,
					processingTimeService,
					checkpointLock,
					streamStatusMaintainer,
					idleTimeout);

				break;
			case ProcessingTime:
				ctx = new NonTimestampContext<>(checkpointLock, output);
				break;
			default:
				throw new IllegalArgumentException(String.valueOf(timeCharacteristic));
		}
		return ctx;
	}

	/**
	 * A source context that attached {@code -1} as a timestamp to all records, and that
	 * does not forward watermarks.
	 */
	/**
	 * 配合 ProcessingTime 方式使用，ts 在 flink 处理完打上
	 * 一个 source context 设置 -1 作为所有 record 的 ts，同时不用向前传递 watermark
	 */
	private static class NonTimestampContext<T> implements SourceFunction.SourceContext<T> {

		private final Object lock;
		private final Output<StreamRecord<T>> output;
		private final StreamRecord<T> reuse;

		private NonTimestampContext(Object checkpointLock, Output<StreamRecord<T>> output) {
			this.lock = Preconditions.checkNotNull(checkpointLock, "The checkpoint lock cannot be null.");
			this.output = Preconditions.checkNotNull(output, "The output cannot be null.");
			this.reuse = new StreamRecord<>(null);
		}

		@Override
		public void collect(T element) {
			synchronized (lock) {
				output.collect(reuse.replace(element));
			}
		}

		@Override
		public void collectWithTimestamp(T element, long timestamp) {
			// ignore the timestamp
			collect(element);
		}

		@Override
		public void emitWatermark(Watermark mark) {
			// do nothing
		}

		@Override
		public void markAsTemporarilyIdle() {
			// do nothing
		}

		@Override
		public Object getCheckpointLock() {
			return lock;
		}

		@Override
		public void close() {}
	}

	/**
	 * {@link SourceFunction.SourceContext} to be used for sources with automatic timestamps
	 * and watermark emission.
	 */
	/**
	 * 配合 IngestionTime 方式使用，ts 在 StreamRecord 进入 flink 的时候打上
	 * 自动 emit timestamps 和 watermark
	 */
	private static class AutomaticWatermarkContext<T> extends WatermarkContext<T> {

		private final Output<StreamRecord<T>> output;
		private final StreamRecord<T> reuse;

		private final long watermarkInterval;

		private volatile ScheduledFuture<?> nextWatermarkTimer;
		private volatile long nextWatermarkTime;

		private long lastRecordTime;

		private AutomaticWatermarkContext(
				final Output<StreamRecord<T>> output,
				final long watermarkInterval,
				final ProcessingTimeService timeService,
				final Object checkpointLock,
				final StreamStatusMaintainer streamStatusMaintainer,
				final long idleTimeout) {

			super(timeService, checkpointLock, streamStatusMaintainer, idleTimeout);

			this.output = Preconditions.checkNotNull(output, "The output cannot be null.");

			Preconditions.checkArgument(watermarkInterval >= 1L, "The watermark interval cannot be smaller than 1 ms.");
			this.watermarkInterval = watermarkInterval;

			this.reuse = new StreamRecord<>(null);

			this.lastRecordTime = Long.MIN_VALUE;

			long now = this.timeService.getCurrentProcessingTime();
			this.nextWatermarkTimer = this.timeService.registerTimer(now + watermarkInterval,
				new WatermarkEmittingTask(this.timeService, checkpointLock, output));
		}

		@Override
		protected void processAndCollect(T element) {
			lastRecordTime = this.timeService.getCurrentProcessingTime();
			output.collect(reuse.replace(element, lastRecordTime));

			// this is to avoid lock contention in the lockingObject by
			// sending the watermark before the firing of the watermark
			// emission task.
			// 捎带传输一波，避免锁竞争
			// 在 emit StreamRecord 的时候，检查当前时间内是否大于下一次发送 watermark 的时候
			// 如果大于，则避免进行 nextWatermarkTimer 的调度，直接 emit 一个 watermark 完事
			if (lastRecordTime > nextWatermarkTime) {
				// in case we jumped some watermarks, recompute the next watermark time
				// 这里需要重新计算，目的是为了下一次 emit watermark 是 watermarkInterval 的整数倍
				final long watermarkTime = lastRecordTime - (lastRecordTime % watermarkInterval);
				nextWatermarkTime = watermarkTime + watermarkInterval;
				output.emitWatermark(new Watermark(watermarkTime));

				// we do not need to register another timer here
				// because the emitting task will do so.
			}
		}

		@Override
		/**
		 * IngestionTime 模式用机器当前时间来设置 ts		
		 */
		protected void processAndCollectWithTimestamp(T element, long timestamp) {
			processAndCollect(element);
		}

		@Override
		/**
		 * 流已经没有数据，发送一个 value 为 LONG.MAX_VALUE 的 watermark 告诉 operators
		 */
		protected boolean allowWatermark(Watermark mark) {
			// allow Long.MAX_VALUE since this is the special end-watermark that for example the Kafka source emits
			return mark.getTimestamp() == Long.MAX_VALUE && nextWatermarkTime != Long.MAX_VALUE;
		}

		/** This will only be called if allowWatermark returned {@code true}. */
		@Override
		protected void processAndEmitWatermark(Watermark mark) {
			nextWatermarkTime = Long.MAX_VALUE;
			output.emitWatermark(mark);

			// we can shutdown the watermark timer now, no watermarks will be needed any more.
			// Note that this procedure actually doesn't need to be synchronized with the lock,
			// but since it's only a one-time thing, doesn't hurt either
			// 发送了 value 为 Long.MAX_VALUE 的最后一个 watermark，不需要再发送 watermark 了，可以 cancel 定时器
			final ScheduledFuture<?> nextWatermarkTimer = this.nextWatermarkTimer;
			if (nextWatermarkTimer != null) {
				nextWatermarkTimer.cancel(true);
			}
		}

		@Override
		public void close() {
			super.close();

			final ScheduledFuture<?> nextWatermarkTimer = this.nextWatermarkTimer;
			if (nextWatermarkTimer != null) {
				nextWatermarkTimer.cancel(true);
			}
		}

		private class WatermarkEmittingTask implements ProcessingTimeCallback {

			private final ProcessingTimeService timeService;
			private final Object lock;
			private final Output<StreamRecord<T>> output;

			private WatermarkEmittingTask(
					ProcessingTimeService timeService,
					Object checkpointLock,
					Output<StreamRecord<T>> output) {
				this.timeService = timeService;
				this.lock = checkpointLock;
				this.output = output;
			}

			@Override
			public void onProcessingTime(long timestamp) {
				// 时间用统一的，避免不同机器时间不对
				final long currentTime = timeService.getCurrentProcessingTime();

				synchronized (lock) {
					// we should continue to automatically emit watermarks if we are active
					// 如果 Stream 是 active 的，我们应该持续自动 emit watermark
					if (streamStatusMaintainer.getStreamStatus().isActive()) {
						if (idleTimeout != -1 && currentTime - lastRecordTime > idleTimeout) {
							// if we are configured to detect idleness, piggy-back the idle detection check on the
							// watermark interval, so that we may possibly discover idle sources faster before waiting
							// for the next idle check to fire
							// 在这里检测有可能能更早的发现 Stream 空闲
							markAsTemporarilyIdle();

							// no need to finish the next check, as we are now idle.
							cancelNextIdleDetectionTask();
						} else if (currentTime > nextWatermarkTime) {
							// align the watermarks across all machines. this will ensure that we
							// don't have watermarks that creep along at different intervals because
							// the machine clocks are out of sync
							final long watermarkTime = currentTime - (currentTime % watermarkInterval);

							output.emitWatermark(new Watermark(watermarkTime));
							nextWatermarkTime = watermarkTime + watermarkInterval;
						}
					}
				}

				long nextWatermark = currentTime + watermarkInterval;
				nextWatermarkTimer = this.timeService.registerTimer(
						nextWatermark, new WatermarkEmittingTask(this.timeService, lock, output));
			}
		}
	}

	/**
	 * A SourceContext for event time. Sources may directly attach timestamps and generate
	 * watermarks, but if records are emitted without timestamps, no timestamps are automatically
	 * generated and attached. The records will simply have no timestamp in that case.
	 *
	 * <p>Streaming topologies can use timestamp assigner functions to override the timestamps
	 * assigned here.
	 */
	/**
	 * 配合 EventTime 方式使用，ts 是 StreamRecord 自带的 
	 */
	private static class ManualWatermarkContext<T> extends WatermarkContext<T> {

		private final Output<StreamRecord<T>> output;
		private final StreamRecord<T> reuse;

		private ManualWatermarkContext(
				final Output<StreamRecord<T>> output,
				final ProcessingTimeService timeService,
				final Object checkpointLock,
				final StreamStatusMaintainer streamStatusMaintainer,
				final long idleTimeout) {

			super(timeService, checkpointLock, streamStatusMaintainer, idleTimeout);

			this.output = Preconditions.checkNotNull(output, "The output cannot be null.");
			this.reuse = new StreamRecord<>(null);
		}

		@Override
		protected void processAndCollect(T element) {
			output.collect(reuse.replace(element));
		}

		@Override
		protected void processAndCollectWithTimestamp(T element, long timestamp) {
			output.collect(reuse.replace(element, timestamp));
		}

		@Override
		protected void processAndEmitWatermark(Watermark mark) {
			output.emitWatermark(mark);
		}

		@Override
		protected boolean allowWatermark(Watermark mark) {
			return true;
		}
	}

	/**
	 * An abstract {@link SourceFunction.SourceContext} that should be used as the base for
	 * stream source contexts that are relevant with {@link Watermark}s.
	 *
	 * <p>Stream source contexts that are relevant with watermarks are responsible of manipulating
	 * the current {@link StreamStatus}, so that stream status can be correctly propagated
	 * downstream. Please refer to the class-level documentation of {@link StreamStatus} for
	 * information on how stream status affects watermark advancement at downstream tasks.
	 *
	 * <p>This class implements the logic of idleness detection. It fires idleness detection
	 * tasks at a given interval; if no records or watermarks were collected by the source context
	 * between 2 consecutive checks, it determines the source to be IDLE and correspondingly
	 * toggles the status. ACTIVE status resumes as soon as some record or watermark is collected
	 * again.
	 */
	/**
	 * 与 watermark 相关的 SourceContext 的抽象类
	 * 与 watermark 有关的 SourceContext 负责控制当前的 StreamStatus，使得 StreamStatus 能够正确的传播到
	 * 下游
	 * 本类实现了空闲检测的逻辑，会在给定的时间间隔开启空闲检测。如果在两次连续的检测中没有 StreamRecord 或者 watermark 被
	 * SourceContext collect，会认为 Source 空闲并且相应的修改 Status，当 StreamRecord 或 watermark 被 collect 之后
	 * Status 立马变为 ACTIVE
	 */
	private abstract static class WatermarkContext<T> implements SourceFunction.SourceContext<T> {

		protected final ProcessingTimeService timeService;
		protected final Object checkpointLock;
		protected final StreamStatusMaintainer streamStatusMaintainer;
		protected final long idleTimeout;

		private ScheduledFuture<?> nextCheck;

		/**
		 * This flag will be reset to {@code true} every time the next check is scheduled.
		 * Whenever a record or watermark is collected, the flag will be set to {@code false}.
		 *
		 * <p>When the scheduled check is fired, if the flag remains to be {@code true}, the check
		 * will fail, and our current status will determined to be IDLE.
		 */
		/**
		 * 用于判断 Stream 是否进入空闲状态，第一次检查的时候会设为 true，在第二次检查之前
		 * 如果 collect StreamRecord 或 watermark，设为 false
		 * 如果在第二次检查的时候还是 true 的话
		 * 说明 Stream 是空闲的
		 */
		private volatile boolean failOnNextCheck;

		/**
		 * Create a watermark context.
		 *
		 * @param timeService the time service to schedule idleness detection tasks
		 * @param checkpointLock the checkpoint lock
		 * @param streamStatusMaintainer the stream status maintainer to toggle and retrieve current status
		 * @param idleTimeout (-1 if idleness checking is disabled)
		 */
		/**
		 * 创建一个 watermark 上下文
		 * @param timeService 时间服务用于调度空闲检测
		 * @param checkpointLock the checkpoint lock，SourceFuntion 中提到 collect 操作需要上锁
		 * @param streamStatusMaintainer 流状态维护器，用于改变和恢复流状态
		 */
		public WatermarkContext(
				final ProcessingTimeService timeService,
				final Object checkpointLock,
				final StreamStatusMaintainer streamStatusMaintainer,
				final long idleTimeout) {

			this.timeService = Preconditions.checkNotNull(timeService, "Time Service cannot be null.");
			this.checkpointLock = Preconditions.checkNotNull(checkpointLock, "Checkpoint Lock cannot be null.");
			this.streamStatusMaintainer = Preconditions.checkNotNull(streamStatusMaintainer, "Stream Status Maintainer cannot be null.");

			if (idleTimeout != -1) {
				Preconditions.checkArgument(idleTimeout >= 1, "The idle timeout cannot be smaller than 1 ms.");
			}
			this.idleTimeout = idleTimeout;

			scheduleNextIdleDetectionTask();
		}

		@Override
		public void collect(T element) {
			synchronized (checkpointLock) {
				streamStatusMaintainer.toggleStreamStatus(StreamStatus.ACTIVE);

				if (nextCheck != null) {
					this.failOnNextCheck = false;
				} else {
					scheduleNextIdleDetectionTask();
				}

				processAndCollect(element);
			}
		}

		@Override
		public void collectWithTimestamp(T element, long timestamp) {
			synchronized (checkpointLock) {
				streamStatusMaintainer.toggleStreamStatus(StreamStatus.ACTIVE);

				if (nextCheck != null) {
					this.failOnNextCheck = false;
				} else {
					scheduleNextIdleDetectionTask();
				}

				processAndCollectWithTimestamp(element, timestamp);
			}
		}

		@Override
		public void emitWatermark(Watermark mark) {
			if (allowWatermark(mark)) {
				synchronized (checkpointLock) {
					streamStatusMaintainer.toggleStreamStatus(StreamStatus.ACTIVE);

					if (nextCheck != null) {
						this.failOnNextCheck = false;
					} else {
						scheduleNextIdleDetectionTask();
					}

					processAndEmitWatermark(mark);
				}
			}
		}

		@Override
		/**
		 * SourceContext 认为当前 Stream 处于空闲状态
		 */
		public void markAsTemporarilyIdle() {
			synchronized (checkpointLock) {
				// 流状态管理器设置流的状态为空闲
				streamStatusMaintainer.toggleStreamStatus(StreamStatus.IDLE);
			}
		}

		@Override
		public Object getCheckpointLock() {
			return checkpointLock;
		}

		@Override
		public void close() {
			cancelNextIdleDetectionTask();
		}

		private class IdlenessDetectionTask implements ProcessingTimeCallback {
			@Override
			public void onProcessingTime(long timestamp) throws Exception {
				synchronized (checkpointLock) {
					// set this to null now;
					// the next idleness detection will be scheduled again
					// depending on the below failOnNextCheck condition
					nextCheck = null;

					if (failOnNextCheck) {
						markAsTemporarilyIdle();
					} else {
						scheduleNextIdleDetectionTask();
					}
				}
			}
		}

		/**
		 * 注册下一次空闲检测的任务
		 */
		private void scheduleNextIdleDetectionTask() {
			if (idleTimeout != -1) {
				// reset flag; if it remains true when task fires, we have detected idleness
				failOnNextCheck = true;
				// 注册下一次空闲检测的时间
				nextCheck = this.timeService.registerTimer(
					this.timeService.getCurrentProcessingTime() + idleTimeout,
					new IdlenessDetectionTask());
			}
		}

		/**
		 * 取消空闲检测
		 */
		protected void cancelNextIdleDetectionTask() {
			final ScheduledFuture<?> nextCheck = this.nextCheck;
			if (nextCheck != null) {
				nextCheck.cancel(true);
			}
		}

		// ------------------------------------------------------------------------
		//	Abstract methods for concrete subclasses to implement.
		//  These methods are guaranteed to be synchronized on the checkpoint lock,
		//  so implementations don't need to do so.
		// ------------------------------------------------------------------------

		/** Process and collect record. */
		protected abstract void processAndCollect(T element);

		/** Process and collect record with timestamp. */
		protected abstract void processAndCollectWithTimestamp(T element, long timestamp);

		/** Whether or not a watermark should be allowed. */
		protected abstract boolean allowWatermark(Watermark mark);

		/**
		 * Process and emit watermark. Only called if
		 * {@link WatermarkContext#allowWatermark(Watermark)} returns {@code true}.
		 */
		protected abstract void processAndEmitWatermark(Watermark mark);

	}
}
