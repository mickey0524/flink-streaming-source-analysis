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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.runtime.state.InternalPriorityQueue;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link InternalTimerService} that stores timers on the Java heap.
 * InternalTimerService 在 Java 堆上存储定时器
 */
public class InternalTimerServiceImpl<K, N> implements InternalTimerService<N>, ProcessingTimeCallback {

	private final ProcessingTimeService processingTimeService;

	/**
	 * Inteface for setting and querying the current key of keyed operations
	 * 设置和获取当前 keyed 操作的 key 的接口
	 * setCurrentKey() 和 getCurrentKey() 两个方法
	 */
	private final KeyContext keyContext;

	/**
	 * Processing time timers that are currently in-flight.
	 */
	/**
	 * 已经注册的进程时间定时器
	 * KeyGroupedInternalPriorityQueue 中有一个方法 getSubsetForKeyGroup(int keyGroupId)
	 * 根据 keyGroupId 从优先级队列中找到属于 keyGroupId 的子集
	 */
	private final KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<K, N>> processingTimeTimersQueue;

	/**
	 * Event time timers that are currently in-flight.
	 */
	/**
	 * 已经注册的时间时间定时器
	 */
	private final KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<K, N>> eventTimeTimersQueue;

	/**
	 * Information concerning the local key-group range.
	 */
	/**
	 * 有关本地 key-group 范围的信息
	 */
	private final KeyGroupRange localKeyGroupRange;

	private final int localKeyGroupRangeStartIdx;

	/**
	 * The local event time, as denoted by the last received
	 * {@link org.apache.flink.streaming.api.watermark.Watermark Watermark}.
	 */
	private long currentWatermark = Long.MIN_VALUE;

	/**
	 * The one and only Future (if any) registered to execute the
	 * next {@link Triggerable} action, when its (processing) time arrives.
	 * */
	/**
	 * 接受定时器的回调
	 */
	private ScheduledFuture<?> nextTimer;

	// Variables to be set when the service is started.

	private TypeSerializer<K> keySerializer;

	private TypeSerializer<N> namespaceSerializer;

	private Triggerable<K, N> triggerTarget;

	private volatile boolean isInitialized;

	private TypeSerializer<K> keyDeserializer;

	private TypeSerializer<N> namespaceDeserializer;

	/** The restored timers snapshot, if any. */
	// 存储定时器快照
	private InternalTimersSnapshot<K, N> restoredTimersSnapshot;

	InternalTimerServiceImpl(
		KeyGroupRange localKeyGroupRange,
		KeyContext keyContext,
		ProcessingTimeService processingTimeService,
		KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<K, N>> processingTimeTimersQueue,
		KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<K, N>> eventTimeTimersQueue) {

		this.keyContext = checkNotNull(keyContext);
		this.processingTimeService = checkNotNull(processingTimeService);
		this.localKeyGroupRange = checkNotNull(localKeyGroupRange);
		this.processingTimeTimersQueue = checkNotNull(processingTimeTimersQueue);
		this.eventTimeTimersQueue = checkNotNull(eventTimeTimersQueue);

		// find the starting index of the local key-group range
		// 寻找本地 key-group 范围开始的下标
		int startIdx = Integer.MAX_VALUE;
		for (Integer keyGroupIdx : localKeyGroupRange) {
			startIdx = Math.min(keyGroupIdx, startIdx);
		}
		this.localKeyGroupRangeStartIdx = startIdx;
	}

	/**
	 * Starts the local {@link InternalTimerServiceImpl} by:
	 * <ol>
	 *     <li>Setting the {@code keySerialized} and {@code namespaceSerializer} for the timers it will contain.</li>
	 *     <li>Setting the {@code triggerTarget} which contains the action to be performed when a timer fires.</li>
	 *     <li>Re-registering timers that were retrieved after recovering from a node failure, if any.</li>
	 * </ol>
	 * This method can be called multiple times, as long as it is called with the same serializers.
	 */
	/**
	 * 开启本地定时器服务：
	 * 1. 给相关定时器设置 keySerialized 和 namespaceSerializer
	 * 2. 给定时器触发的时候有后续操作的设置 triggerTarget
	 * 3. 重启失败的定时器
	 */
	public void startTimerService(
			TypeSerializer<K> keySerializer,
			TypeSerializer<N> namespaceSerializer,
			Triggerable<K, N> triggerTarget) {
		
		// InternalTimerServiceManager 里 getInternalTimerService 函数会多次调用 startTimerService
		// 所以这里需要判断
		if (!isInitialized) {

			if (keySerializer == null || namespaceSerializer == null) {
				throw new IllegalArgumentException("The TimersService serializers cannot be null.");
			}

			if (this.keySerializer != null || this.namespaceSerializer != null || this.triggerTarget != null) {
				throw new IllegalStateException("The TimerService has already been initialized.");
			}

			// the following is the case where we restore
			// 从快照中恢复
			if (restoredTimersSnapshot != null) {
				TypeSerializerSchemaCompatibility<K> keySerializerCompatibility =
					restoredTimersSnapshot.getKeySerializerSnapshot().resolveSchemaCompatibility(keySerializer);

				if (keySerializerCompatibility.isIncompatible() || keySerializerCompatibility.isCompatibleAfterMigration()) {
					throw new IllegalStateException(
						"Tried to initialize restored TimerService with new key serializer that requires migration or is incompatible.");
				}

				TypeSerializerSchemaCompatibility<N> namespaceSerializerCompatibility =
					restoredTimersSnapshot.getNamespaceSerializerSnapshot().resolveSchemaCompatibility(namespaceSerializer);

				if (namespaceSerializerCompatibility.isIncompatible() || namespaceSerializerCompatibility.isCompatibleAfterMigration()) {
					throw new IllegalStateException(
						"Tried to initialize restored TimerService with new namespace serializer that requires migration or is incompatible.");
				}

				this.keySerializer = keySerializerCompatibility.isCompatibleAsIs()
					? keySerializer : keySerializerCompatibility.getReconfiguredSerializer();
				this.namespaceSerializer = namespaceSerializerCompatibility.isCompatibleAsIs()
					? namespaceSerializer : namespaceSerializerCompatibility.getReconfiguredSerializer();
			} else {
				this.keySerializer = keySerializer;
				this.namespaceSerializer = namespaceSerializer;
			}

			this.keyDeserializer = null;
			this.namespaceDeserializer = null;

			this.triggerTarget = Preconditions.checkNotNull(triggerTarget);

			// re-register the restored timers (if any)
			// 如果存在的话，重新注册存储的定时器
			final InternalTimer<K, N> headTimer = processingTimeTimersQueue.peek();
			if (headTimer != null) {
				nextTimer = processingTimeService.registerTimer(headTimer.getTimestamp(), this);
			}
			this.isInitialized = true;
		} else {
			if (!(this.keySerializer.equals(keySerializer) && this.namespaceSerializer.equals(namespaceSerializer))) {
				throw new IllegalArgumentException("Already initialized Timer Service " +
					"tried to be initialized with different key and namespace serializers.");
			}
		}
	}

	@Override
	// 当前的进程时间
	public long currentProcessingTime() {
		return processingTimeService.getCurrentProcessingTime();
	}

	@Override
	// 当前的 watermark
	public long currentWatermark() {
		return currentWatermark;
	}

	@Override
	// 注册进程时间定时器
	public void registerProcessingTimeTimer(N namespace, long time) {
		InternalTimer<K, N> oldHead = processingTimeTimersQueue.peek();
		if (processingTimeTimersQueue.add(new TimerHeapInternalTimer<>(time, (K) keyContext.getCurrentKey(), namespace))) {
			// 获取当前堆顶的定时器触发时间
			long nextTriggerTime = oldHead != null ? oldHead.getTimestamp() : Long.MAX_VALUE;
			// check if we need to re-schedule our timer to earlier
			// 检查我们是否需要更早的调度定时器
			if (time < nextTriggerTime) {
				if (nextTimer != null) {
					nextTimer.cancel(false);
				}
				// 重新注册定时器，注册在 processingTimeService 上
				nextTimer = processingTimeService.registerTimer(time, this);
			}
		}
	}

	@Override
	// 注册事件时间定时器
	public void registerEventTimeTimer(N namespace, long time) {
		eventTimeTimersQueue.add(new TimerHeapInternalTimer<>(time, (K) keyContext.getCurrentKey(), namespace));
	}

	@Override
	// 删除进程时间定时器
	public void deleteProcessingTimeTimer(N namespace, long time) {
		processingTimeTimersQueue.remove(new TimerHeapInternalTimer<>(time, (K) keyContext.getCurrentKey(), namespace));
	}

	@Override
	// 删除事件时间定时器
	public void deleteEventTimeTimer(N namespace, long time) {
		eventTimeTimersQueue.remove(new TimerHeapInternalTimer<>(time, (K) keyContext.getCurrentKey(), namespace));
	}

	@Override
	// 当进程时间定时器触发的时候，执行的方法，ProcessingTimeCallback 的方法
	public void onProcessingTime(long time) throws Exception {
		// null out the timer in case the Triggerable calls registerProcessingTimeTimer()
		// inside the callback.
		// 先将 nextTimer 设置为 null
		// 防止 triggerTarget.onProcessingTime 调用 registerProcessingTimeTimer
		// 最后再在 processingTimeService 上注册新的定时器
		nextTimer = null;

		InternalTimer<K, N> timer;

		// 将所有进程时间定时器的 ts <= time 的都触发
		while ((timer = processingTimeTimersQueue.peek()) != null && timer.getTimestamp() <= time) {
			processingTimeTimersQueue.poll();
			keyContext.setCurrentKey(timer.getKey());
			triggerTarget.onProcessingTime(timer);
		}

		if (timer != null && nextTimer == null) {
			nextTimer = processingTimeService.registerTimer(timer.getTimestamp(), this);
		}
	}

	// 更新 watermark，InternalTimeServiceManager 中 advanceWatermark 方法统一调用
	public void advanceWatermark(long time) throws Exception {
		currentWatermark = time;

		InternalTimer<K, N> timer;
		// 查看是否 eventTimeTimersQueue 有可以触发的定时器
		while ((timer = eventTimeTimersQueue.peek()) != null && timer.getTimestamp() <= time) {
			eventTimeTimersQueue.poll();
			keyContext.setCurrentKey(timer.getKey());
			triggerTarget.onEventTime(timer);
		}
	}

	/**
	 * Snapshots the timers (both processing and event time ones) for a given {@code keyGroupIdx}.
	 *
	 * @param keyGroupIdx the id of the key-group to be put in the snapshot.
	 * @return a snapshot containing the timers for the given key-group, and the serializers for them
	 */
	public InternalTimersSnapshot<K, N> snapshotTimersForKeyGroup(int keyGroupIdx) {
		return new InternalTimersSnapshot<>(
			keySerializer,
			namespaceSerializer,
			eventTimeTimersQueue.getSubsetForKeyGroup(keyGroupIdx),
			processingTimeTimersQueue.getSubsetForKeyGroup(keyGroupIdx));
	}

	public TypeSerializer<K> getKeySerializer() {
		return keySerializer;
	}

	public TypeSerializer<N> getNamespaceSerializer() {
		return namespaceSerializer;
	}

	/**
	 * Restore the timers (both processing and event time ones) for a given {@code keyGroupIdx}.
	 *
	 * @param restoredSnapshot the restored snapshot containing the key-group's timers,
	 *                       and the serializers that were used to write them
	 * @param keyGroupIdx the id of the key-group to be put in the snapshot.
	 */
	@SuppressWarnings("unchecked")
	public void restoreTimersForKeyGroup(InternalTimersSnapshot<?, ?> restoredSnapshot, int keyGroupIdx) {
		this.restoredTimersSnapshot = (InternalTimersSnapshot<K, N>) restoredSnapshot;

		TypeSerializer<K> restoredKeySerializer = restoredTimersSnapshot.getKeySerializerSnapshot().restoreSerializer();
		if (this.keyDeserializer != null && !this.keyDeserializer.equals(restoredKeySerializer)) {
			throw new IllegalArgumentException("Tried to restore timers for the same service with different key serializers.");
		}
		this.keyDeserializer = restoredKeySerializer;

		TypeSerializer<N> restoredNamespaceSerializer = restoredTimersSnapshot.getNamespaceSerializerSnapshot().restoreSerializer();
		if (this.namespaceDeserializer != null && !this.namespaceDeserializer.equals(restoredNamespaceSerializer)) {
			throw new IllegalArgumentException("Tried to restore timers for the same service with different namespace serializers.");
		}
		this.namespaceDeserializer = restoredNamespaceSerializer;

		checkArgument(localKeyGroupRange.contains(keyGroupIdx),
			"Key Group " + keyGroupIdx + " does not belong to the local range.");

		// restore the event time timers
		eventTimeTimersQueue.addAll(restoredTimersSnapshot.getEventTimeTimers());

		// restore the processing time timers
		processingTimeTimersQueue.addAll(restoredTimersSnapshot.getProcessingTimeTimers());
	}

	@VisibleForTesting
	// 当前进程时间定时器的数量
	public int numProcessingTimeTimers() {
		return this.processingTimeTimersQueue.size();
	}

	@VisibleForTesting
	// 当前事件时间定时器的数量
	public int numEventTimeTimers() {
		return this.eventTimeTimersQueue.size();
	}

	@VisibleForTesting
	// 命名空间为 namespace 的进程时间定时器的数量
	public int numProcessingTimeTimers(N namespace) {
		return countTimersInNamespaceInternal(namespace, processingTimeTimersQueue);
	}

	@VisibleForTesting
	// 命名空间为 namespace 的事件时间定时器的数量
	public int numEventTimeTimers(N namespace) {
		return countTimersInNamespaceInternal(namespace, eventTimeTimersQueue);
	}

	// 获取对应命名空间的定时器数量
	private int countTimersInNamespaceInternal(N namespace, InternalPriorityQueue<TimerHeapInternalTimer<K, N>> queue) {
		int count = 0;
		try (final CloseableIterator<TimerHeapInternalTimer<K, N>> iterator = queue.iterator()) {
			while (iterator.hasNext()) {
				final TimerHeapInternalTimer<K, N> timer = iterator.next();
				if (timer.getNamespace().equals(namespace)) {
					count++;
				}
			}
		} catch (Exception e) {
			throw new FlinkRuntimeException("Exception when closing iterator.", e);
		}
		return count;
	}

	@VisibleForTesting
	int getLocalKeyGroupRangeStartIdx() {
		return this.localKeyGroupRangeStartIdx;
	}

	@VisibleForTesting
	List<Set<TimerHeapInternalTimer<K, N>>> getEventTimeTimersPerKeyGroup() {
		return partitionElementsByKeyGroup(eventTimeTimersQueue);
	}

	@VisibleForTesting
	List<Set<TimerHeapInternalTimer<K, N>>> getProcessingTimeTimersPerKeyGroup() {
		return partitionElementsByKeyGroup(processingTimeTimersQueue);
	}

	// 通过 key 划分元素
	private <T> List<Set<T>> partitionElementsByKeyGroup(KeyGroupedInternalPriorityQueue<T> keyGroupedQueue) {
		List<Set<T>> result = new ArrayList<>(localKeyGroupRange.getNumberOfKeyGroups());
		for (int keyGroup : localKeyGroupRange) {
			result.add(Collections.unmodifiableSet(keyGroupedQueue.getSubsetForKeyGroup(keyGroup)));
		}
		return result;
	}
}
