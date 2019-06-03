/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.windowing.assigners;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.Collection;
import java.util.Collections;

/**
 * A {@link WindowAssigner} that windows elements into windows based on the current
 * system time of the machine the operation is running on. Windows cannot overlap.
 *
 * <p>For example, in order to window into windows of 1 minute, every 10 seconds:
 * <pre> {@code
 * DataStream<Tuple2<String, Integer>> in = ...;
 * KeyedStream<String, Tuple2<String, Integer>> keyed = in.keyBy(...);
 * WindowedStream<Tuple2<String, Integer>, String, TimeWindows> windowed =
 *   keyed.window(TumblingProcessingTimeWindows.of(Time.of(1, MINUTES), Time.of(10, SECONDS));
 * } </pre>
 */
/**
 * 一种窗口分配器，根据运行操作的机器的当前系统时间，将元素放入窗口中，窗口不能重叠
 * 翻转进程时间窗口，例如窗口 size 为 60，offset 为 0
 * 0 - 60 为第一个窗口，60 - 120 为第二个，依此类推
 */
public class TumblingProcessingTimeWindows extends WindowAssigner<Object, TimeWindow> {
	private static final long serialVersionUID = 1L;

	private final long size;  // 窗口大小

	private final long offset;  // 窗口偏移

	private TumblingProcessingTimeWindows(long size, long offset) {
		// 当 offset 大于 size 的时候，抛出异常
		if (Math.abs(offset) >= size) {
			throw new IllegalArgumentException("TumblingProcessingTimeWindows parameters must satisfy abs(offset) < size");
		}

		this.size = size;
		this.offset = offset;
	}

	@Override
	// 根据当前进程的时间分配窗口，与 element 本身无关，只取决的于当前的进程时间
	public Collection<TimeWindow> assignWindows(Object element, long timestamp, WindowAssignerContext context) {
		final long now = context.getCurrentProcessingTime();
		// 获取元素应该位于窗口的开端
		long start = TimeWindow.getWindowStartWithOffset(now, offset, size);
		return Collections.singletonList(new TimeWindow(start, start + size));
	}

	/**
	 * 返回窗口长度
	 */
	public long getSize() {
		return size;
	}

	@Override
	public Trigger<Object, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
		return ProcessingTimeTrigger.create();
	}

	@Override
	public String toString() {
		return "TumblingProcessingTimeWindows(" + size + ")";
	}

	/**
	 * Creates a new {@code TumblingProcessingTimeWindows} {@link WindowAssigner} that assigns
	 * elements to time windows based on the element timestamp.
	 *
	 * @param size The size of the generated windows.
	 * @return The time policy.
	 */
	/**
	 * 创建一个 TumblingProcessingTimeWindows，依据元素的 ts 来分配元素到哪个窗口
	 */
	public static TumblingProcessingTimeWindows of(Time size) {
		return new TumblingProcessingTimeWindows(size.toMilliseconds(), 0);
	}

	/**
	 * Creates a new {@code TumblingProcessingTimeWindows} {@link WindowAssigner} that assigns
	 * elements to time windows based on the element timestamp and offset.
	 *
	 * <p>For example, if you want window a stream by hour,but window begins at the 15th minutes
	 * of each hour, you can use {@code of(Time.hours(1),Time.minutes(15))},then you will get
	 * time windows start at 0:15:00,1:15:00,2:15:00,etc.
	 *
	 * <p>Rather than that,if you are living in somewhere which is not using UTC±00:00 time,
	 * such as China which is using UTC+08:00,and you want a time window with size of one day,
	 * and window begins at every 00:00:00 of local time,you may use {@code of(Time.days(1),Time.hours(-8))}.
	 * The parameter of offset is {@code Time.hours(-8))} since UTC+08:00 is 8 hours earlier than UTC time.
	 *
	 * @param size The size of the generated windows.
	 * @param offset The offset which window start would be shifted by.
	 * @return The time policy.
	 */
	/**
	 * 创建一个 TumblingProcessingTimeWindows，依据元素的 ts 和分配器的 offset 来分配元素到哪个窗口
	 * 
	 * 例如，如果你想按小时来窗口化一个流，但是窗口在每小时的第 15 分钟开启，你可以使用 of(Time.hours(1),Time.minutes(15))
	 * 这样你会得到在 0:15:00,1:15:00,2:15:00... 开始的时间窗口
	 */
	public static TumblingProcessingTimeWindows of(Time size, Time offset) {
		return new TumblingProcessingTimeWindows(size.toMilliseconds(), offset.toMilliseconds());
	}

	@Override
	public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
		return new TimeWindow.Serializer();
	}

	@Override
	public boolean isEventTime() {
		return false;
	}
}
