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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * The {@code AssignerWithPeriodicWatermarks} assigns event time timestamps to elements,
 * and generates low watermarks that signal event time progress within the stream.
 * These timestamps and watermarks are used by functions and operators that operate
 * on event time, for example event time windows.
 *
 * AssignerWithPeriodicWatermarks 将事件时间时间戳分配给元素，并生成低水位标记
 * 以指示流中的事件时间进度。这些时间戳和水印由在事件时间操作的功能和操作员使用，例如事件时间窗口
 * 
 * <p>Use this class to generate watermarks in a periodical interval.
 * At most every {@code i} milliseconds (configured via
 * {@link ExecutionConfig#getAutoWatermarkInterval()}), the system will call the
 * {@link #getCurrentWatermark()} method to probe for the next watermark value.
 * The system will generate a new watermark, if the probed value is non-null
 * and has a timestamp larger than that of the previous watermark (to preserve
 * the contract of ascending watermarks).
 *
 * 使用此类以周期性间隔生成水印。最多每隔1毫秒(通过配置 getAutoWatermarkInterval())
 * 系统将调用 getCurrentWatermark() 方法来探测下一个水印值
 * 如果探测值非空并且时间戳大于先前水印的时间戳（保留上升水印原则），系统将生成新水印
 * 
 * <p>The system may call the {@link #getCurrentWatermark()} method less often than every
 * {@code i} milliseconds, if no new elements arrived since the last call to the
 * method.
 *
 * 如果自上次调用方法后没有新元素到达，系统可以比每隔1毫秒更缓地调用getCurrentWatermark（）方法
 *
 * <p>Timestamps and watermarks are defined as {@code longs} that represent the
 * milliseconds since the Epoch (midnight, January 1, 1970 UTC).
 * A watermark with a certain value {@code t} indicates that no elements with event
 * timestamps {@code x}, where {@code x} is lower or equal to {@code t}, will occur any more.
 *
 * 时间戳和水印定义是 long，表示自纪元（1970年1月1日午夜）以来的毫秒数
 * 具有特定值 t 的水印表示没有具有事件时间戳 x 的元素，其中 x 小于或等于 t，将再次出现
 *
 * @param <T> The type of the elements to which this assigner assigns timestamps.
 *
 * @see org.apache.flink.streaming.api.watermark.Watermark
 */
public interface AssignerWithPeriodicWatermarks<T> extends TimestampAssigner<T> {

	/**
	 * Returns the current watermark. This method is periodically called by the
	 * system to retrieve the current watermark. The method may return {@code null} to
	 * indicate that no new Watermark is available.
	 *
	 * 返回当前水印。系统周期性地调用该方法以检索当前水印。该方法可以返回 null 以指示没有新的水印可用
	 *
	 * <p>The returned watermark will be emitted only if it is non-null and its timestamp
	 * is larger than that of the previously emitted watermark (to preserve the contract of
	 * ascending watermarks). If the current watermark is still
	 * identical to the previous one, no progress in event time has happened since
	 * the previous call to this method. If a null value is returned, or the timestamp
	 * of the returned watermark is smaller than that of the last emitted one, then no
	 * new watermark will be generated.
	 *
	 * 返回的水印只有在非空且其时间戳大于先前发出的水印的时间戳时才会被发射（以保持上升水印的合同）
	 * 如果当前水印仍然与前一个水印相同，则自上次调用此方法以来事件时间没有进展。
	 * 如果返回空值，或者返回的水印的时间戳小于上次发出的水印的时间戳，则不会生成新的水印
	 *
	 * <p>The interval in which this method is called and Watermarks are generated
	 * depends on {@link ExecutionConfig#getAutoWatermarkInterval()}.
	 *
	 * @see org.apache.flink.streaming.api.watermark.Watermark
	 * @see ExecutionConfig#getAutoWatermarkInterval()
	 *
	 * @return {@code Null}, if no watermark should be emitted, or the next watermark to emit.
	 */
	@Nullable
	Watermark getCurrentWatermark();
}
