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

package org.apache.flink.streaming.api.operators.async.queue;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.operators.async.OperatorActions;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Unordered implementation of the {@link StreamElementQueue}. The unordered stream element queue
 * emits asynchronous results as soon as they are completed. Additionally it maintains the
 * watermark-stream record order. This means that no stream record can be overtaken by a watermark
 * and no watermark can overtake a stream record. However, stream records falling in the same
 * segment between two watermarks can overtake each other (their emission order is not guaranteed).
 */
/**
 * StreamElementQueue 的无序实现
 * 无序流元素队列在完成后立即 emit 异步结果
 * 此外，它还保持水印流记录顺序，这意味着没有流记录可以被水印超过，也没有水印可以超过流元素
 * 这是通过 firstSet、lastSet 以及 uncompletedQueue 来实现的
 * 但是，落在两个水印之间的同一段中的流记录可能会相互超越（不保证其 emit 顺序）
 */
@Internal
public class UnorderedStreamElementQueue implements StreamElementQueue {

	private static final Logger LOG = LoggerFactory.getLogger(UnorderedStreamElementQueue.class);

	/** Capacity of this queue. */
	// 队列的容量
	private final int capacity;

	/** Executor to run the onComplete callbacks. */
	// 执行 onComplete 回调的 Executor
	private final Executor executor;

	/** OperatorActions to signal the owning operator a failure. */
	// OperatorActions 向所属操作符的发出故障信号
	private final OperatorActions operatorActions;

	/** Queue of uncompleted stream element queue entries segmented by watermarks. */
	// 由水印分段的未完成的流元素队列条目的队列
	private final ArrayDeque<Set<StreamElementQueueEntry<?>>> uncompletedQueue;

	/** Queue of completed stream element queue entries. */
	// 已完成的流元素队列条目的队列
	private final ArrayDeque<StreamElementQueueEntry<?>> completedQueue;

	/** First (chronologically oldest) uncompleted set of stream element queue entries. */
	// 第一个（按时间顺序排列最早的）未完成的流元素队列条目集
	private Set<StreamElementQueueEntry<?>> firstSet;

	// Last (chronologically youngest) uncompleted set of stream element queue entries. New
	// stream element queue entries are inserted into this set.
	// 最后（按时间顺序排列最晚）未完成的流元素队列条目集
	// 新的流元素队列条目将插入此集合中
	private Set<StreamElementQueueEntry<?>> lastSet;
	private volatile int numberEntries;

	/** Locks and conditions for the blocking queue. */
	// 锁和条件，用于阻塞队列
	private final ReentrantLock lock;
	private final Condition notFull;
	private final Condition hasCompletedEntries;

	public UnorderedStreamElementQueue(
			int capacity,
			Executor executor,
			OperatorActions operatorActions) {

		Preconditions.checkArgument(capacity > 0, "The capacity must be larger than 0.");
		this.capacity = capacity;

		this.executor = Preconditions.checkNotNull(executor, "executor");

		this.operatorActions = Preconditions.checkNotNull(operatorActions, "operatorActions");

		this.uncompletedQueue = new ArrayDeque<>(capacity);
		this.completedQueue = new ArrayDeque<>(capacity);

		// 最开始的时候，firstSet 和 lastSet 指向的是同一块内存地址
		this.firstSet = new HashSet<>(capacity);
		this.lastSet = firstSet;

		this.numberEntries = 0;

		this.lock = new ReentrantLock();
		this.notFull = lock.newCondition();
		this.hasCompletedEntries = lock.newCondition();
	}

	// 插入一个 StreamElementQueueEntry，如果队列满，阻塞
	@Override
	public <T> void put(StreamElementQueueEntry<T> streamElementQueueEntry) throws InterruptedException {
		lock.lockInterruptibly();

		try {
			// while 阻塞，notFull 条件唤醒，当 numberEntries 小于 capacity 才能添加
			while (numberEntries >= capacity) {
				notFull.await();
			}

			addEntry(streamElementQueueEntry);
		} finally {
			lock.unlock();
		}
	}

	// 插入一个 StreamElementQueueEntry，如果队列满，返回 false
	@Override
	public <T> boolean tryPut(StreamElementQueueEntry<T> streamElementQueueEntry) throws InterruptedException {
		lock.lockInterruptibly();

		try {
			// 不阻塞，队列有空余就添加，否则就删除
			if (numberEntries < capacity) {
				addEntry(streamElementQueueEntry);

				LOG.debug("Put element into unordered stream element queue. New filling degree " +
					"({}/{}).", numberEntries, capacity);

				return true;
			} else {
				LOG.debug("Failed to put element into unordered stream element queue because it " +
					"was full ({}/{}).", numberEntries, capacity);

				return false;
			}
		} finally {
			lock.unlock();
		}
	}

	@Override
	// peek 只查看，不删除
	public AsyncResult peekBlockingly() throws InterruptedException {
		lock.lockInterruptibly();

		try {
			// 用 hasCompletedEntries 条件阻塞
			while (completedQueue.isEmpty()) {
				hasCompletedEntries.await();
			}

			LOG.debug("Peeked head element from unordered stream element queue with filling degree " +
				"({}/{}).", numberEntries, capacity);

			return completedQueue.peek();
		} finally {
			lock.unlock();
		}
	}

	@Override
	// poll 删除
	public AsyncResult poll() throws InterruptedException {
		lock.lockInterruptibly();

		try {
			// 同样用 hasCompletedEntries 条件阻塞
			while (completedQueue.isEmpty()) {
				hasCompletedEntries.await();
			}

			numberEntries--;
			notFull.signalAll();

			LOG.debug("Polled element from unordered stream element queue. New filling degree " +
				"({}/{}).", numberEntries, capacity);

			return completedQueue.poll();
		} finally {
			lock.unlock();
		}
	}

	/**
	 * 返回队列中所有的 StreamElementQueueEntry 组成的集合
	 */
	@Override
	public Collection<StreamElementQueueEntry<?>> values() throws InterruptedException {
		lock.lockInterruptibly();

		try {
			StreamElementQueueEntry<?>[] array = new StreamElementQueueEntry[numberEntries];

			array = completedQueue.toArray(array);  // 先将 completedQueue 中的元素放进去

			int counter = completedQueue.size();

			// 然后将 firstSet 中的元素放进去
			for (StreamElementQueueEntry<?> entry: firstSet) {
				array[counter] = entry;
				counter++;
			}
			
			// 最后将 uncompletedQueue 中元素放进去
			for (Set<StreamElementQueueEntry<?>> asyncBufferEntries : uncompletedQueue) {

				for (StreamElementQueueEntry<?> streamElementQueueEntry : asyncBufferEntries) {
					array[counter] = streamElementQueueEntry;
					counter++;
				}
			}

			return Arrays.asList(array);
		} finally {
			lock.unlock();
		}
	}

	@Override
	public boolean isEmpty() {
		return numberEntries == 0;
	}

	@Override
	public int size() {
		return numberEntries;
	}

	/**
	 * Callback for onComplete events for the given stream element queue entry. Whenever a queue
	 * entry is completed, it is checked whether this entry belongs to the first set. If this is the
	 * case, then the element is added to the completed entries queue from where it can be consumed.
	 * If the first set becomes empty, then the next set is polled from the uncompleted entries
	 * queue. Completed entries from this new set are then added to the completed entries queue.
	 *
	 * @param streamElementQueueEntry which has been completed
	 * @throws InterruptedException if the current thread has been interrupted while performing the
	 * 	on complete callback.
	 */
	/**
	 * 回调给定流元素队列条目的 onComplete 事件
	 * 每当队列条目完成时，检查该条目是否属于第一组，如果是这种情况，则将元素添加到已完成的条目队列中，从中可以使用该元素
	 * 如果第一个集合变为空，则从未完成的条目队列中轮询下一个集
	 * 然后，将来自此新集的已完成条目添加到已完成的条目队列中
	 */
	public void onCompleteHandler(StreamElementQueueEntry<?> streamElementQueueEntry) throws InterruptedException {
		lock.lockInterruptibly();

		try {
			if (firstSet.remove(streamElementQueueEntry)) {
				completedQueue.offer(streamElementQueueEntry);
				// 当从 firstSet 中 remove 一个元素，然后 firstSet 变为空
				// 说明 firstSet 中存放的是一个 watermark
				// watermark 完成了，当前 uncompletedQueue 中的元素也应该完成了
				// 当 firstSet 重新指向 lastSet 的时候，跳出循环
				while (firstSet.isEmpty() && firstSet != lastSet) {
					firstSet = uncompletedQueue.poll();

					Iterator<StreamElementQueueEntry<?>> it = firstSet.iterator();

					while (it.hasNext()) {
						StreamElementQueueEntry<?> bufferEntry = it.next();

						if (bufferEntry.isDone()) {
							completedQueue.offer(bufferEntry);
							it.remove();
						}
					}
				}

				LOG.debug("Signal unordered stream element queue has completed entries.");
				hasCompletedEntries.signalAll();
			}
		} finally {
			lock.unlock();
		}
	}

	/**
	 * Add the given stream element queue entry to the current last set if it is not a watermark.
	 * If it is a watermark, then stop adding to the current last set, insert the watermark into its
	 * own set and add a new last set.
	 *
	 * @param streamElementQueueEntry to be inserted
	 * @param <T> Type of the stream element queue entry's result
	 */
	/**
	 * 如果给定的流元素队列条目不是水印，则将其添加到当前的最后一个集合
	 * 如果它是水印，则停止添加到当前的最后一组，将水印插入其自己的集合中并添加新的最后一组
	 */
	private <T> void addEntry(StreamElementQueueEntry<T> streamElementQueueEntry) {
		assert(lock.isHeldByCurrentThread());

		if (streamElementQueueEntry.isWatermark()) {
			lastSet = new HashSet<>(capacity);

			if (firstSet.isEmpty()) {
				firstSet.add(streamElementQueueEntry);
			} else {
				Set<StreamElementQueueEntry<?>> watermarkSet = new HashSet<>(1);
				watermarkSet.add(streamElementQueueEntry);
				uncompletedQueue.offer(watermarkSet);
			}
			uncompletedQueue.offer(lastSet);  // uncompletedQueue 插入新的 lastSet，随后来的 StreamElement 都会直接写入 uncompletedQueue
		} else {
			lastSet.add(streamElementQueueEntry);
		}

		streamElementQueueEntry.onComplete(
			(StreamElementQueueEntry<T> value) -> {
				try {
					onCompleteHandler(value);
				} catch (InterruptedException e) {
					// The accept executor thread got interrupted. This is probably cause by
					// the shutdown of the executor.
					LOG.debug("AsyncBufferEntry could not be properly completed because the " +
						"executor thread has been interrupted.", e);
				} catch (Throwable t) {
					operatorActions.failOperator(new Exception("Could not complete the " +
						"stream element queue entry: " + value + '.', t));
				}
			},
			executor);

		numberEntries++;
	}
}
