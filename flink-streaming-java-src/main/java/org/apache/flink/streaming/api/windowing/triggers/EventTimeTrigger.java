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

package org.apache.flink.streaming.api.windowing.triggers;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * A {@link Trigger} that fires once the watermark passes the end of the window
 * to which a pane belongs.
 *
 * @see org.apache.flink.streaming.api.watermark.Watermark
 */
/**
 * 一种触发器，当 watermark 超过窗格所属窗口的末端时候触发
 */
@PublicEvolving
public class EventTimeTrigger extends Trigger<Object, TimeWindow> {
	private static final long serialVersionUID = 1L;

	private EventTimeTrigger() {}

	@Override
	public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
		if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
			// if the watermark is already past the window fire immediately
			// 如果 watermark 已经超过窗口末端了，立即触发定时器
			return TriggerResult.FIRE;
		} else {
			// 否则注册事件时间定时器，在 onEventTime 中触发
			ctx.registerEventTimeTimer(window.maxTimestamp());
			return TriggerResult.CONTINUE;
		}
	}

	@Override
	public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) {
		return time == window.maxTimestamp() ?
			TriggerResult.FIRE :
			TriggerResult.CONTINUE;
	}

	@Override
	public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
		return TriggerResult.CONTINUE;
	}

	@Override
	public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
		ctx.deleteEventTimeTimer(window.maxTimestamp());
	}

	@Override
	public boolean canMerge() {
		return true;
	}

	@Override
	public void onMerge(TimeWindow window,
			OnMergeContext ctx) {
		// only register a timer if the watermark is not yet past the end of the merged window
		// this is in line with the logic in onElement(). If the watermark is past the end of
		// the window onElement() will fire and setting a timer here would fire the window twice.
		// 只有当 watermark 还没有超过合并窗口的末端时才注册定时器
		// 这里与 onElement() 中的逻辑是一样的
		// 如果 watermark 超过了窗口的末端，onElement() 将触发，在此设置计时器将触发窗口两次
		long windowMaxTimestamp = window.maxTimestamp();
		if (windowMaxTimestamp > ctx.getCurrentWatermark()) {
			ctx.registerEventTimeTimer(windowMaxTimestamp);
		}
	}

	@Override
	public String toString() {
		return "EventTimeTrigger()";
	}

	/**
	 * Creates an event-time trigger that fires once the watermark passes the end of the window.
	 *
	 * <p>Once the trigger fires all elements are discarded. Elements that arrive late immediately
	 * trigger window evaluation with just this one element.
	 */
	/**
	 * 创建一种触发器，当 watermark 超过窗格所属窗口的末端时候触发
     *
	 * 一旦触发器触发，所有元素都将被丢弃。延迟到达的元素立即触发窗口执行，窗口中仅有一个元素
	 */
	public static EventTimeTrigger create() {
		return new EventTimeTrigger();
	}
}
