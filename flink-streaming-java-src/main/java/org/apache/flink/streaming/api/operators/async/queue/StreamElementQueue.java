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
import org.apache.flink.streaming.api.operators.async.AsyncWaitOperator;

import java.util.Collection;

/**
 * Interface for blocking stream element queues for the {@link AsyncWaitOperator}.
 */
/**
 * 用于阻塞 AsyncWaitOperator 的流元素队列的接口
 */
@Internal
public interface StreamElementQueue {

	/**
	 * Put the given element in the queue if capacity is left. If not, then block until this is
	 * the case.
	 *
	 * @param streamElementQueueEntry to be put into the queue
	 * @param <T> Type of the entries future value
	 * @throws InterruptedException if the calling thread has been interrupted while waiting to
	 * 	insert the given element
	 */
	/**
	 * 将 streamElementQueueEntry 参数加入队列，如果队列满了，则阻塞直到队列有空余
	 */
	<T> void put(StreamElementQueueEntry<T> streamElementQueueEntry) throws InterruptedException;

	/**
	 * Try to put the given element in the queue. This operation succeeds if the queue has capacity
	 * left and fails if the queue is full.
	 *
	 * @param streamElementQueueEntry to be inserted
	 * @param <T> Type of the entries future value
	 * @return True if the entry could be inserted; otherwise false
	 * @throws InterruptedException if the calling thread has been interrupted while waiting to
	 * 	insert the given element
	 */
	/**
	 * 尝试将 streamElementQueueEntry 加入队列，加入成功返回 true，失败返回 false
	 */
	<T> boolean tryPut(StreamElementQueueEntry<T> streamElementQueueEntry) throws InterruptedException;

	/**
	 * Peek at the head of the queue and return the first completed {@link AsyncResult}. This
	 * operation is a blocking operation and only returns once a completed async result has been
	 * found.
	 *
	 * @return Completed {@link AsyncResult}
	 * @throws InterruptedException if the current thread has been interrupted while waiting for a
	 * 	completed async result.
	 */
	/**
	 * 查看队列的头部并返回第一个完成的 AsyncResult
	 * 此操作是阻塞操作，只有在找到完成的异步结果后才会返回
	 */
	AsyncResult peekBlockingly() throws InterruptedException;

	/**
	 * Poll the first completed {@link AsyncResult} from the head of this queue. This operation is
	 * blocking and only returns once a completed async result has been found.
	 *
	 * @return Completed {@link AsyncResult} which has been removed from the queue
	 * @throws InterruptedException if the current thread has been interrupted while waiting for a
	 * 	completed async result.
	 */
	/**
	 * 从该队列的头部找到第一个完成的 AsyncResult，并 remove
	 * 此操作是阻塞的，只有在找到完成的异步结果后才会返回
	 */
	AsyncResult poll() throws InterruptedException;

	/**
	 * Return the collection of {@link StreamElementQueueEntry} currently contained in this queue.
	 *
	 * @return Collection of currently contained {@link StreamElementQueueEntry}.
	 * @throws InterruptedException if the current thread has been interrupted while retrieving the
	 * 	stream element queue entries of this queue.
	 */
	/**
	 * 返回当前队列中包含的 StreamElementQueueEntry 的集合
	 */
	Collection<StreamElementQueueEntry<?>> values() throws InterruptedException;

	/**
	 * True if the queue is empty; otherwise false.
	 *
	 * @return True if the queue is empty; otherwise false.
	 */
	/**
	 * 返回队列是否是空的
	 */
	boolean isEmpty();

	/**
	 * Return the size of the queue.
	 *
	 * @return The number of elements contained in this queue.
	 */
	/**
	 * 返回队列的大小
	 */
	int size();
}
