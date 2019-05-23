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
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

/**
 * Asynchronous result returned by the {@link StreamElementQueue}. The asynchronous result can
 * either be a {@link Watermark} or a collection of new output elements produced by the
 * {@link AsyncFunction}.
 */
/**
 * StreamElementQueue 返回的异步结果
 * 异步结果可以是 Watermark 或 AsyncFunction 生成的新输出元素的集合
 */
@Internal
public interface AsyncResult {

	/**
	 * True if the async result is a {@link Watermark}; otherwise false.
	 *
	 * @return True if the async result is a {@link Watermark}; otherwise false.
	 */
	/**
	 * 返回异步结果是否是 Watermark
	 */
	boolean isWatermark();

	/**
	 * True if the async result is a collection of output elements; otherwise false.
	 *
	 * @return True if the async result is a collection of output elements; otherwise false
	 */
	/**
	 * 返回异步结果是否是输出元素的集合
	 */
	boolean isResultCollection();

	/**
	 * Return this async result as a async watermark result.
	 *
	 * @return this result as a {@link AsyncWatermarkResult}.
	 */
	/**
	 * 将此异步结果作为异步水印返回
	 */
	AsyncWatermarkResult asWatermark();

	/**
	 * Return this async result as a async result collection.
	 *
	 * @param <T> Type of the result collection's elements
	 * @return this result as a {@link AsyncCollectionResult}.
	 */
	/**
	 * 将此异步结果作为输出元素的集合返回
	 */
	<T> AsyncCollectionResult<T> asResultCollection();
}
