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

package org.apache.flink.streaming.api.windowing.evictors;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;

import java.io.Serializable;

/**
 * An {@code Evictor} can remove elements from a pane before/after the evaluation of WindowFunction
 * and after the window evaluation gets triggered by a
 * {@link org.apache.flink.streaming.api.windowing.triggers.Trigger}
 *
 * <p>A pane is the bucket of elements that have the same key (assigned by the
 * {@link org.apache.flink.api.java.functions.KeySelector}) and same {@link Window}. An element can
 * be in multiple panes of it was assigned to multiple windows by the
 * {@link org.apache.flink.streaming.api.windowing.assigners.WindowAssigner}. These panes all
 * have their own instance of the {@code Evictor}.
 *
 * @param <T> The type of elements that this {@code Evictor} can evict.
 * @param <W> The type of {@link Window Windows} on which this {@code Evictor} can operate.
 */
/**
 * 一个 Evictor 可以在窗口函数执行前后清空窗格内的元素，也可以在触发器触发窗口执行后做相同的事情
 *
 * 一个窗格是一个具有相同 key 和分配了相同 window 的元素组成的桶，一个元素可以位于多个窗格内
 * 因为一个元素可以被分配多个窗口，窗格都有它们自己的 Evictor
 */
@PublicEvolving
public interface Evictor<T, W extends Window> extends Serializable {

	/**
	 * Optionally evicts elements. Called before windowing function.
	 *
	 * @param elements The elements currently in the pane.
	 * @param size The current number of elements in the pane.
	 * @param window The {@link Window}
	 * @param evictorContext The context for the Evictor
     */
	/**
	 * 可选的驱逐元素，在窗口函数执行前调用
	 */
	void evictBefore(Iterable<TimestampedValue<T>> elements, int size, W window, EvictorContext evictorContext);

	/**
	 * Optionally evicts elements. Called after windowing function.
	 *
	 * @param elements The elements currently in the pane.
	 * @param size The current number of elements in the pane.
	 * @param window The {@link Window}
	 * @param evictorContext The context for the Evictor
	 */
	/**
	 * 可选的驱逐元素，在窗口函数执行后调用
	 */
	void evictAfter(Iterable<TimestampedValue<T>> elements, int size, W window, EvictorContext evictorContext);


	/**
	 * A context object that is given to {@link Evictor} methods.
	 */
	/**
	 * Evictor 方法的上下文对象
	 */
	interface EvictorContext {

		/**
		 * Returns the current processing time.
		 */
		/**
		 * 返回当前的进程时间
		 */
		long getCurrentProcessingTime();

		/**
		 * Returns the metric group for this {@link Evictor}. This is the same metric
		 * group that would be returned from {@link RuntimeContext#getMetricGroup()} in a user
		 * function.
		 *
		 * <p>You must not call methods that create metric objects
		 * (such as {@link MetricGroup#counter(int)} multiple times but instead call once
		 * and store the metric object in a field.
		 */
		MetricGroup getMetricGroup();

		/**
		 * Returns the current watermark time.
		 */
		/**
		 * 返回当前的 watermark
		 */
		long getCurrentWatermark();
	}
}

