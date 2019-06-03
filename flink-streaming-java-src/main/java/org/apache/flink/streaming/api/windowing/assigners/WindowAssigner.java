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

package org.apache.flink.streaming.api.windowing.assigners;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.Window;

import java.io.Serializable;
import java.util.Collection;

/**
 * A {@code WindowAssigner} assigns zero or more {@link Window Windows} to an element.
 *
 * <p>In a window operation, elements are grouped by their key (if available) and by the windows to
 * which it was assigned. The set of elements with the same key and window is called a pane.
 * When a {@link Trigger} decides that a certain pane should fire the
 * {@link org.apache.flink.streaming.api.functions.windowing.WindowFunction} is applied
 * to produce output elements for that pane.
 *
 * @param <T> The type of elements that this WindowAssigner can assign windows to.
 * @param <W> The type of {@code Window} that this assigner assigns.
 */
/**
 * 一个窗口分配器将一个或多个窗口分配给一个元素
 *
 * 在一个窗口操作中，元素按其键（如果可用）和分配给它的窗口分组。具有相同键和窗口的元素集称为窗格
 * 当一个触发器决定一个特定的窗格应该被触发，窗口函数会作用在窗格上来输出元素
 */
@PublicEvolving
public abstract class WindowAssigner<T, W extends Window> implements Serializable {
	private static final long serialVersionUID = 1L;

	/**
	 * Returns a {@code Collection} of windows that should be assigned to the element.
	 *
	 * @param element The element to which windows should be assigned.
	 * @param timestamp The timestamp of the element.
	 * @param context The {@link WindowAssignerContext} in which the assigner operates.
	 */
	/**
	 * 返回应该分配给元素的窗口集合
	 *
	 * @param element 被分配窗口的元素
	 * @param timestamp 元素的 ts
	 * @param context 分配器在其中操作的 WindowAssignerContext
	 */
	public abstract Collection<W> assignWindows(T element, long timestamp, WindowAssignerContext context);

	/**
	 * Returns the default trigger associated with this {@code WindowAssigner}.
	 */
	/**
	 * 返回与该 WindowAssigner 有关的默认触发器
	 */
	public abstract Trigger<T, W> getDefaultTrigger(StreamExecutionEnvironment env);

	/**
	 * Returns a {@link TypeSerializer} for serializing windows that are assigned by
	 * this {@code WindowAssigner}.
	 */
	/**
	 * 返回一个类型序列器，用来序列化窗口
	 */
	public abstract TypeSerializer<W> getWindowSerializer(ExecutionConfig executionConfig);

	/**
	 * Returns {@code true} if elements are assigned to windows based on event time,
	 * {@code false} otherwise.
	 */
	/**
	 * 返回元素是否是按照 event time 来分配窗口的
	 */
	public abstract boolean isEventTime();

	/**
	 * A context provided to the {@link WindowAssigner} that allows it to query the
	 * current processing time.
	 *
	 * <p>This is provided to the assigner by its containing
	 * {@link org.apache.flink.streaming.runtime.operators.windowing.WindowOperator},
	 * which, in turn, gets it from the containing
	 * {@link org.apache.flink.streaming.runtime.tasks.StreamTask}.
	 */
	/**
	 * WindowAssigner 的 context，可以用于请求当前的进程时间
	 */
	public abstract static class WindowAssignerContext {

		/**
		 * Returns the current processing time.
		 */
		/**
		 * 返回当前的进程时间
		 */
		public abstract long getCurrentProcessingTime();

	}
}
