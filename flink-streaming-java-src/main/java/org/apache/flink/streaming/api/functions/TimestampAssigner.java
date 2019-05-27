/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions;

import org.apache.flink.api.common.functions.Function;

/**
 * A {@code TimestampAssigner} assigns event time timestamps to elements.
 * These timestamps are used by all functions that operate on event time,
 * for example event time windows.
 *
 * <p>Timestamps are represented in milliseconds since the Epoch
 * (midnight, January 1, 1970 UTC).
 *
 * @param <T> The type of the elements to which this assigner assigns timestamps.
 */
/**
 * TimestampAssigner 将事件时间时间戳分配给元素
 * 这些时间戳由在事件时间上操作的所有函数使用，例如事件时间窗口
 */
public interface TimestampAssigner<T> extends Function {

	/**
	 * Assigns a timestamp to an element, in milliseconds since the Epoch.
	 *
	 * <p>The method is passed the previously assigned timestamp of the element.
	 * That previous timestamp may have been assigned from a previous assigner,
	 * by ingestion time. If the element did not carry a timestamp before, this value is
	 * {@code Long.MIN_VALUE}.
	 *
	 * @param element The element that the timestamp will be assigned to.
	 * @param previousElementTimestamp The previous internal timestamp of the element,
	 *                                 or a negative value, if no timestamp has been assigned yet.
	 * @return The new timestamp.
	 */
	/**
	 * 给元素分配时间戳
	 *
	 * 该方法传递先前分配的元素时间戳
	 * 之前的 ts 有可能通过之前的分配器分配，例如 ingestion time
	 * 如果元素没有携带一个之前分配的 ts，这个值为 Long.MIN_VALUE
	 */
	long extractTimestamp(T element, long previousElementTimestamp);
}
