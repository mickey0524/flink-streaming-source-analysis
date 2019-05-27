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
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * {@link StreamElementQueueEntry} implementation for {@link StreamRecord}. This class also acts
 * as the {@link ResultFuture} implementation which is given to the {@link AsyncFunction}. The
 * async function completes this class with a collection of results.
 *
 * @param <OUT> Type of the asynchronous collection result
 */
/**
 * StreamRecord 的 StreamElementQueueEntry 实现
 * 此类还充当为 AsyncFunction 提供的 ResultFuture 实现
 * 异步函数使用一组结果完成此类
 */
@Internal
public class StreamRecordQueueEntry<OUT> extends StreamElementQueueEntry<Collection<OUT>>
	implements AsyncCollectionResult<OUT>, ResultFuture<OUT> {

	/** Timestamp information. */
	// 时间戳信息
	private final boolean hasTimestamp;
	private final long timestamp;

	/** Future containing the collection result. */
	// 包含集合结果的 Future
	private final CompletableFuture<Collection<OUT>> resultFuture;

	public StreamRecordQueueEntry(StreamRecord<?> streamRecord) {
		super(streamRecord);

		hasTimestamp = streamRecord.hasTimestamp();
		timestamp = streamRecord.getTimestamp();

		resultFuture = new CompletableFuture<>();
	}

	@Override
	public boolean hasTimestamp() {
		return hasTimestamp;
	}

	@Override
	public long getTimestamp() {
		return timestamp;
	}

	@Override
	public Collection<OUT> get() throws Exception {
		return resultFuture.get();
	}

	@Override
	protected CompletableFuture<Collection<OUT>> getFuture() {
		return resultFuture;
	}

	/**
	 * userFunction 中调用这个方法，设置 CompletableFuture 的 result
	 * 这样 CompletableFuture.isDone() 返回就会返回 true
	 */
	@Override
	public void complete(Collection<OUT> result) {
		resultFuture.complete(result);
	}

	@Override
	public void completeExceptionally(Throwable error) {
		resultFuture.completeExceptionally(error);
	}
}
