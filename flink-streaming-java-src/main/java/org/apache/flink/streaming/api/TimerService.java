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

package org.apache.flink.streaming.api;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Interface for working with time and timers.
 */
/**
 * 与时间和定时器相关的接口
 */
@PublicEvolving
public interface TimerService {

	/** Error string for {@link UnsupportedOperationException} on registering timers. */
	String UNSUPPORTED_REGISTER_TIMER_MSG = "Setting timers is only supported on a keyed streams.";

	/** Error string for {@link UnsupportedOperationException} on deleting timers. */
	String UNSUPPORTED_DELETE_TIMER_MSG = "Deleting timers is only supported on a keyed streams.";

	/** Returns the current processing time. */
	/**
	 * 返回当前的进程时间
	 */
	long currentProcessingTime();

	/** Returns the current event-time watermark. */
	/**
	 * 返回当前的 event-time watermark
	 */
	long currentWatermark();

	/**
	 * Registers a timer to be fired when processing time passes the given time.
	 *
	 * <p>Timers can internally be scoped to keys and/or windows. When you set a timer
	 * in a keyed context, such as in an operation on
	 * {@link org.apache.flink.streaming.api.datastream.KeyedStream} then that context
	 * will also be active when you receive the timer notification.
	 */
	/**
	 * 注册一个定时器，当进程时间大于给定的 time 参数的时候，定时器被触发
	 * 定时器可能作用在 keys 和 windows 的内部
	 * 当你在分区上下文中设置了一个定时器
	 * 当你收到定时器通知的时候，上下文也是可以用的
	 */
	void registerProcessingTimeTimer(long time);

	/**
	 * Registers a timer to be fired when the event time watermark passes the given time.
	 *
	 * <p>Timers can internally be scoped to keys and/or windows. When you set a timer
	 * in a keyed context, such as in an operation on
	 * {@link org.apache.flink.streaming.api.datastream.KeyedStream} then that context
	 * will also be active when you receive the timer notification.
	 */
	/**
	 * 注册一个定时器，当 event-time watermark 时间大于给定的 time 参数的时候，定时器被触发
	 * 定时器可能作用在 keys 和 windows 的内部
	 * 当你在分区上下文中设置了一个定时器
	 * 当你收到定时器通知的时候，上下文也是可以用的
	 */
	void registerEventTimeTimer(long time);

	/**
	 * Deletes the processing-time timer with the given trigger time. This method has only an effect if such a timer
	 * was previously registered and did not already expire.
	 *
	 * <p>Timers can internally be scoped to keys and/or windows. When you delete a timer,
	 * it is removed from the current keyed context.
	 */
	/**
	 * 根据给出的触发时间删除进程时间定时器
	 * 只有当定时器先前被注册了并且还未触发的时候本方法有作用
	 * 当你在分区上下文中设置了一个定时器，然后执行本方法将其删除之后
	 * 它会从当前的 keyed 上下文中被移除
	 */
	void deleteProcessingTimeTimer(long time);

	/**
	 * Deletes the event-time timer with the given trigger time. This method has only an effect if such a timer
	 * was previously registered and did not already expire.
	 *
	 * <p>Timers can internally be scoped to keys and/or windows. When you delete a timer,
	 * it is removed from the current keyed context.
	 */
	/**
	 * 根据给出的触发时间删除事件时间定时器
	 * 只有当定时器先前被注册了并且还未触发的时候本方法有作用
	 * 当你在分区上下文中设置了一个定时器，然后执行本方法将其删除之后
	 * 它会从当前的 keyed 上下文中被移除
	 */
	void deleteEventTimeTimer(long time);
}
