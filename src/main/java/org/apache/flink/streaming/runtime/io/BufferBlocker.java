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
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;

import java.io.IOException;

/**
 * The buffer blocker takes the buffers and events from a data stream and adds them in a sequence.
 * After a number of elements have been added, the blocker can "roll over": It presents the added
 * elements as a readable sequence, and creates a new sequence.
 */
/**
 * BufferBlocker 从数据流中获取缓冲区和事件，并按顺序添加它们
 * 添加了许多元素后，BufferBlocker 可以"翻转"
 * 它将添加的元素作为可读序列呈现，并创建新序列
 */
@Internal
public interface BufferBlocker {

	/**
	 * Adds a buffer or event to the blocker.
	 *
	 * @param boe The buffer or event to be added into the blocker.
	 */
	/**
	 * 向 blocker 中添加一个 buffer 或一个 event
	 */
	void add(BufferOrEvent boe) throws IOException;

	/**
	 * Starts a new sequence of buffers and event without reusing the same resources and
	 * returns the current sequence of buffers for reading.
	 *
	 * @return The readable sequence of buffers and events, or 'null', if nothing was added.
	 */
	/**
	 * 启动一个新的缓冲区和事件序列，而不重用相同的资源，并返回当前的缓冲区序列以供读取
	 */
	BufferOrEventSequence rollOverWithoutReusingResources() throws IOException;

	/**
	 * Starts a new sequence of buffers and event reusing the same resources and
	 * returns the current sequence of buffers for reading.
	 *
	 * @return The readable sequence of buffers and events, or 'null', if nothing was added.
	 */
	/**
	 * 启动一个新的缓冲区和事件序列，重用相同的资源，并返回当前的缓冲区序列以供读取
	 */
	BufferOrEventSequence rollOverReusingResources() throws IOException;

	/**
	 * Cleans up all the resources in the current sequence.
	 */
	/**
	 * 清空当前序列中的全部资源
	 */
	void close() throws IOException;

	/**
	 * Gets the number of bytes blocked in the current sequence.
	 *
	 * @return the number of bytes blocked in the current sequence.
	 */
	/**
	 * 获取当前序列中缓存的字节数
	 */
	long getBytesBlocked();
}
