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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Preconditions;

import java.util.Comparator;
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This is a {@link ProcessingTimeService} used <b>strictly for testing</b> the
 * processing time functionality.
 */
/**
 * 这是一个严格用于测试处理时间功能的ProcessingTimeService
 */
public class TestProcessingTimeService extends ProcessingTimeService {

	private volatile long currentTime = Long.MIN_VALUE;  // 当前时间

	private volatile boolean isTerminated;  // 是否处于终止状态
	private volatile boolean isQuiesced;  // 是否处于停顿状态

	// sorts the timers by timestamp so that they are processed in the correct order.
	// 按时间戳对计时器进行排序，以便按正确的顺序处理它们
	private final PriorityQueue<Tuple2<Long, CallbackTask>> priorityQueue;

	public TestProcessingTimeService() {
		this.priorityQueue = new PriorityQueue<>(16, new Comparator<Tuple2<Long, CallbackTask>>() {
			@Override
			public int compare(Tuple2<Long, CallbackTask> o1, Tuple2<Long, CallbackTask> o2) {
				return Long.compare(o1.f0, o2.f0);
			}
		});
	}

	// 设置当前的时间
	public void setCurrentTime(long timestamp) throws Exception {
		this.currentTime = timestamp;

		if (!isQuiesced) {
			while (!priorityQueue.isEmpty() && currentTime >= priorityQueue.peek().f0) {
				Tuple2<Long, CallbackTask> entry = priorityQueue.poll();

				CallbackTask callbackTask = entry.f1;

				if (!callbackTask.isDone()) {
					callbackTask.onProcessingTime(entry.f0);

					if (callbackTask instanceof PeriodicCallbackTask) {
						priorityQueue.offer(Tuple2.of(((PeriodicCallbackTask) callbackTask).nextTimestamp(entry.f0), callbackTask));
					}
				}
			}
		}
	}

	@Override
	// 获取当前进程时间
	public long getCurrentProcessingTime() {
		return currentTime;
	}

	@Override
	// 注册定时器
	public ScheduledFuture<?> registerTimer(long timestamp, ProcessingTimeCallback target) {
		if (isTerminated) {
			throw new IllegalStateException("terminated");
		}
		if (isQuiesced) {
			return new CallbackTask(null);
		}

		CallbackTask callbackTask = new CallbackTask(target);

		priorityQueue.offer(Tuple2.of(timestamp, callbackTask));

		return callbackTask;
	}

	@Override
	public ScheduledFuture<?> scheduleAtFixedRate(ProcessingTimeCallback callback, long initialDelay, long period) {
		if (isTerminated) {
			throw new IllegalStateException("terminated");
		}
		if (isQuiesced) {
			return new CallbackTask(null);
		}

		PeriodicCallbackTask periodicCallbackTask = new PeriodicCallbackTask(callback, period);

		priorityQueue.offer(Tuple2.<Long, CallbackTask>of(currentTime + initialDelay, periodicCallbackTask));

		return periodicCallbackTask;
	}

	@Override
	// 是否终止
	public boolean isTerminated() {
		return isTerminated;
	}

	@Override
	// 停顿
	public void quiesce() {
		if (!isTerminated) {
			isQuiesced = true;
			priorityQueue.clear();
		}
	}

	@Override
	public void awaitPendingAfterQuiesce() throws InterruptedException {
		// do nothing.
	}

	@Override
	// 关闭服务
	public void shutdownService() {
		this.isTerminated = true;
	}

	@Override
	public boolean shutdownServiceUninterruptible(long timeoutMs) {
		shutdownService();
		return true;
	}

	@Override
	public boolean shutdownAndAwaitPending(long time, TimeUnit timeUnit) throws InterruptedException {
		shutdownService();
		return true;
	}

	// 获取当前等待的定时器个数
	public int getNumActiveTimers() {
		int count = 0;

		for (Tuple2<Long, CallbackTask> entry : priorityQueue) {
			if (!entry.f1.isDone()) {
				count++;
			}
		}

		return count;
	}

	// 获取定时器的触发时间
	public Set<Long> getActiveTimerTimestamps() {
		Set<Long> actualTimestamps = new HashSet<>();

		for (Tuple2<Long, CallbackTask> entry : priorityQueue) {
			if (!entry.f1.isDone()) {
				actualTimestamps.add(entry.f0);
			}
		}

		return actualTimestamps;
	}

	// ------------------------------------------------------------------------

	private static class CallbackTask implements ScheduledFuture<Object> {

		protected final ProcessingTimeCallback processingTimeCallback;  // 进程定时器回调

		private AtomicReference<CallbackTaskState> state = new AtomicReference<>(CallbackTaskState.CREATED);

		private CallbackTask(ProcessingTimeCallback processingTimeCallback) {
			this.processingTimeCallback = processingTimeCallback;
		}

		public void onProcessingTime(long timestamp) throws Exception {
			processingTimeCallback.onProcessingTime(timestamp);

			state.compareAndSet(CallbackTaskState.CREATED, CallbackTaskState.DONE);
		}

		@Override
		public long getDelay(TimeUnit unit) {
			throw new UnsupportedOperationException();
		}

		@Override
		public int compareTo(Delayed o) {
			throw new UnsupportedOperationException();
		}

		@Override
		// 取消 CallbackTask
		public boolean cancel(boolean mayInterruptIfRunning) {
			return state.compareAndSet(CallbackTaskState.CREATED, CallbackTaskState.CANCELLED);
		}

		@Override
		// 是否处于取消状态
		public boolean isCancelled() {
			return state.get() == CallbackTaskState.CANCELLED;
		}

		@Override
		// 是否处于完成状态
		public boolean isDone() {
			return state.get() != CallbackTaskState.CREATED;
		}

		@Override
		public Object get() throws InterruptedException, ExecutionException {
			throw new UnsupportedOperationException();
		}

		@Override
		public Object get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
			throw new UnsupportedOperationException();
		}

		// CallbackTask 的状态
		enum CallbackTaskState {
			CREATED,
			CANCELLED,
			DONE
		}
	}

	// 周期性的 CallbackTask
	private static class PeriodicCallbackTask extends CallbackTask {

		private final long period;

		private PeriodicCallbackTask(ProcessingTimeCallback processingTimeCallback, long period) {
			super(processingTimeCallback);
			Preconditions.checkArgument(period > 0L, "The period must be greater than 0.");

			this.period = period;
		}

		@Override
		public void onProcessingTime(long timestamp) throws Exception {
			processingTimeCallback.onProcessingTime(timestamp);
		}

		public long nextTimestamp(long currentTimestamp) {
			return currentTimestamp + period;
		}
	}
}
