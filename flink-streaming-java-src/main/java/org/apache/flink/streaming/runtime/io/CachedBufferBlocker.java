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
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;

import javax.annotation.Nullable;

import java.util.ArrayDeque;

/**
 * The cached buffer blocker takes the buffers and events from a data stream and adds them to a memory queue.
 * After a number of elements have been cached, the blocker can "roll over": It presents the cached
 * elements as a readable sequence, and creates a new memory queue.
 *
 * <p>This buffer blocked can be used in credit-based flow control for better barrier alignment in exactly-once mode.
 */
/**
 * CachedBufferBlocker 从数据流中获取缓冲区和事件，并将它们添加到内存队列中
 * 在缓存了许多元素之后，阻塞程序进行 "翻转"
 * 它将缓存的元素作为可读序列呈现，并创建一个新的内存队列
 * 
 * CachedBufferBlocker 可用于基于信用的流量控制，以便在 exactly-once 下实现更好的屏障对齐
 */
@Internal
public class CachedBufferBlocker implements BufferBlocker {

	/** The page size, to estimate the total cached data size. */
	// 页面大小，用于估计总缓存数据大小
	private final int pageSize;

	/** The number of bytes cached since the last roll over. */
	// 自上次翻转以来缓存的字节数
	private long bytesBlocked;

	/** The current memory queue for caching the buffers or events. */
	// 用于缓存缓冲区或事件的当前内存队列
	private ArrayDeque<BufferOrEvent> currentBuffers;

	/**
	 * Creates a new buffer blocker, caching the buffers or events in memory queue.
	 *
	 * @param pageSize The page size used to estimate the cached size.
	 */
	/**
	 * 创建一个新的 CachedBufferBlocker，缓存内存队列中的缓冲区或事件
	 */
	public CachedBufferBlocker(int pageSize) {
		this.pageSize = pageSize;
		this.currentBuffers = new ArrayDeque<BufferOrEvent>();
	}

	/**
	 * 添加一个 BufferOrEvent
	 */
	@Override
	public void add(BufferOrEvent boe) {
		bytesBlocked += pageSize;

		currentBuffers.add(boe);
	}

	/**
	 * It is never reusing resources and is defaulting to {@link #rollOverWithoutReusingResources()}.
	 */
	/**
	 * CachedBufferBlocker 不重用资源
	 */
	@Override
	public BufferOrEventSequence rollOverReusingResources() {
		return rollOverWithoutReusingResources();
	}

	// 反转，获取缓存的队列
	@Override
	public BufferOrEventSequence rollOverWithoutReusingResources() {
		if (bytesBlocked == 0) {
			return null;
		}

		CachedBufferOrEventSequence currentSequence = new CachedBufferOrEventSequence(currentBuffers, bytesBlocked);
		currentBuffers = new ArrayDeque<BufferOrEvent>();
		bytesBlocked = 0L;

		return currentSequence;
	}

	@Override
	public void close() {
		BufferOrEvent boe;
		while ((boe = currentBuffers.poll()) != null) {
			if (boe.isBuffer()) {
				boe.getBuffer().recycleBuffer();
			}
		}
	}

	@Override
	public long getBytesBlocked() {
		return bytesBlocked;
	}

	// ------------------------------------------------------------------------

	/**
	 * This class represents a sequence of cached buffers and events, created by the
	 * {@link CachedBufferBlocker}.
	 */
	/**
	 * 此类表示由 CachedBufferBlocker 创建的缓存缓冲区和事件序列
	 */
	public static class CachedBufferOrEventSequence implements BufferOrEventSequence {

		/** The sequence of buffers and events to be consumed by {@link BarrierBuffer}.*/
		private final ArrayDeque<BufferOrEvent> queuedBuffers;

		/** The total size of the cached data. */
		private final long size;

		/**
		 * Creates a reader that reads a sequence of buffers and events.
		 *
		 * @param size The total size of cached data.
		 */
		CachedBufferOrEventSequence(ArrayDeque<BufferOrEvent> buffers, long size) {
			this.queuedBuffers = buffers;
			this.size = size;
		}

		@Override
		public void open() {}

		@Override
		@Nullable
		public BufferOrEvent getNext() {
			return queuedBuffers.poll();
		}

		@Override
		public void cleanup() {
			BufferOrEvent boe;
			while ((boe = queuedBuffers.poll()) != null) {
				if (boe.isBuffer()) {
					boe.getBuffer().recycleBuffer();
				}
			}
		}

		@Override
		public long size() {
			return size;
		}
	}
}
