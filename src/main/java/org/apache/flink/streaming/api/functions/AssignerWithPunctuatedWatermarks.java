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

import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * The {@code AssignerWithPunctuatedWatermarks} assigns event time timestamps to elements,
 * and generates low watermarks that signal event time progress within the stream.
 * These timestamps and watermarks are used by functions and operators that operate
 * on event time, for example event time windows.
 *
 * AssignerWithPunctuatedWatermarks将事件时间时间戳分配给元素，并生成低水印，用于指示流中的事件时间进度
 * 这些时间戳和水印由在事件时间操作的功能和操作员使用，例如事件时间窗口
 *
 * <p>Use this class if certain special elements act as markers that signify event time
 * progress, and when you want to emit watermarks specifically at certain events.
 * The system will generate a new watermark, if the probed value is non-null
 * and has a timestamp larger than that of the previous watermark (to preserve
 * the contract of ascending watermarks).
 *
 * 如果某些特殊元素充当表示事件时间进度的标记，并且您希望在特定事件中专门发出水印，则使用此类。
 * 如果探测值非空并且时间戳大于先前水印的时间戳（保留上升水印合约），系统将生成新水印
 *
 * <p>For use cases that should periodically emit watermarks based on element timestamps,
 * use the {@link AssignerWithPeriodicWatermarks} instead.
 *
 * 对于应根据元素时间戳定期发出水印的用例，请改用 AssignerWithPeriodicWatermarks
 *
 * <p>The following example illustrates how to use this timestamp extractor and watermark
 * generator. It assumes elements carry a timestamp that describes when they were created,
 * and that some elements carry a flag, marking them as the end of a sequence such that no
 * elements with smaller timestamps can come anymore.
 *
 * 以下示例说明如何使用此时间戳提取器和水印生成器。它假定元素带有一个时间戳，描述它们何时被创建
 * 并且一些元素带有一个标志，将它们标记为序列的结尾，这样就不会再出现具有较小时间戳的元素
 *
 * <pre>{@code
 * public class WatermarkOnFlagAssigner implements AssignerWithPunctuatedWatermarks<MyElement> {
 *
 *     public long extractTimestamp(MyElement element, long previousElementTimestamp) {
 *         return element.getSequenceTimestamp();
 *     }
 *
 *     public Watermark checkAndGetNextWatermark(MyElement lastElement, long extractedTimestamp) {
 *         return lastElement.isEndOfSequence() ? new Watermark(extractedTimestamp) : null;
 *     }
 * }
 * }</pre>
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
public interface AssignerWithPunctuatedWatermarks<T> extends TimestampAssigner<T> {

	/**
	 * Asks this implementation if it wants to emit a watermark. This method is called right after
	 * the {@link #extractTimestamp(Object, long)} method.
	 *
	 * 如果想要发出水印，请调用此方法。在extractTimestamp（Object，long）方法之后立即调用此方法
	 *
	 * <p>The returned watermark will be emitted only if it is non-null and its timestamp
	 * is larger than that of the previously emitted watermark (to preserve the contract of
	 * ascending watermarks). If a null value is returned, or the timestamp of the returned
	 * watermark is smaller than that of the last emitted one, then no new watermark will
	 * be generated.
	 *
	 * 返回的水印只有在非空且其时间戳大于先前发出的水印的时间戳时才会被发射（以保持上升水印的合同）。
	 * 如果返回空值，或者返回的水印的时间戳小于上次发出的水印的时间戳，则不会生成新的水印
	 *
	 * <p>For an example how to use this method, see the documentation of
	 * {@link AssignerWithPunctuatedWatermarks this class}.
	 *
	 * @return {@code Null}, if no watermark should be emitted, or the next watermark to emit.
	 */
	@Nullable
	Watermark checkAndGetNextWatermark(T lastElement, long extractedTimestamp);
}
