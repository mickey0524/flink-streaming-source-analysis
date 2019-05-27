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

import org.apache.flink.annotation.Internal;

/**
 * Interface for working with time and timers.
 *
 * <p>This is the internal version of {@link org.apache.flink.streaming.api.TimerService}
 * that allows to specify a key and a namespace to which timers should be scoped.
 *
 * @param <N> Type of the namespace to which timers are scoped.
 */
/**
 * 与时间和定时器有关的接口
 */
@Internal
public interface InternalTimerService<N> {

	/** Returns the current processing time. */
	// 返回当前的进程时间
	long currentProcessingTime();

	/** Returns the current event-time watermark. */
	// 返回当前的 event-time watermark
	long currentWatermark();

	/**
	 * Registers a timer to be fired when processing time passes the given time. The namespace
	 * you pass here will be provided when the timer fires.
	 */
	/**
	 * 注册一个定时器，当进程时间大于给定的时间的时候，触发定时器
	 * 当定时器触发的时候 namespace 参数会被提供
	 */
	void registerProcessingTimeTimer(N namespace, long time);

	/**
	 * Deletes the timer for the given key and namespace.
	 */
	/**
	 * 删除进程定时器
	 */
	void deleteProcessingTimeTimer(N namespace, long time);

	/**
	 * Registers a timer to be fired when event time watermark passes the given time. The namespace
	 * you pass here will be provided when the timer fires.
	 */
	/**
	 * 注册一个定时器，当 event-time watermark 的 ts 大于给定的时间的时候触发
	 * 当定时器触发的时候 namespace 参数会被提供
	 */
	void registerEventTimeTimer(N namespace, long time);

	/**
	 * Deletes the timer for the given key and namespace.
	 */
	/**
	 * 删除时间定时器
	 */
	void deleteEventTimeTimer(N namespace, long time);
}
