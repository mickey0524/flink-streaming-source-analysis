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
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MergingState;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.windowing.windows.Window;

import java.io.Serializable;

/**
 * A {@code Trigger} determines when a pane of a window should be evaluated to emit the
 * results for that part of the window.
 *
 * 触发器决定何时计算窗口的窗格以发出该窗口部分的结果
 *
 * <p>A pane is the bucket of elements that have the same key (assigned by the
 * {@link org.apache.flink.api.java.functions.KeySelector}) and same {@link Window}. An element can
 * be in multiple panes if it was assigned to multiple windows by the
 * {@link org.apache.flink.streaming.api.windowing.assigners.WindowAssigner}. These panes all
 * have their own instance of the {@code Trigger}.
 *
 * 一个窗格是一个元素桶，桶内的元素都有相同的 key(KeySelector 分配的) 以及相同的窗口
 * 一个元素可以被分配多个窗口，因此一个元素可能存在于多个窗格
 * 这些窗格都有他们自己的 Trigger 实例
 *
 * <p>Triggers must not maintain state internally since they can be re-created or reused for
 * different keys. All necessary state should be persisted using the state abstraction
 * available on the {@link TriggerContext}.
 *
 * 触发器不能在内部维护状态，因为它们可能被不同的 key 重新创建或者重用
 * 所有必要的状态都应该使用 TriggerContext 上可用的状态抽象来持久化
 *
 * <p>When used with a {@link org.apache.flink.streaming.api.windowing.assigners.MergingWindowAssigner}
 * the {@code Trigger} must return {@code true} from {@link #canMerge()} and
 * {@link #onMerge(Window, OnMergeContext)} most be properly implemented.
 *
 * 当和 MergingWindowAssigner 一起使用的时候
 * canMerge 需要返回 true
 * onMerge 方法需要被实现
 *
 * @param <T> The type of elements on which this {@code Trigger} works.
 * @param <W> The type of {@link Window Windows} on which this {@code Trigger} can operate.
 */
@PublicEvolving
public abstract class Trigger<T, W extends Window> implements Serializable {

	private static final long serialVersionUID = -4104633972991191369L;

	/**
	 * Called for every element that gets added to a pane. The result of this will determine
	 * whether the pane is evaluated to emit results.
	 *
	 * @param element The element that arrived.
	 * @param timestamp The timestamp of the element that arrived.
	 * @param window The window to which the element is being added.
	 * @param ctx A context object that can be used to register timer callbacks.
	 */
	/**
	 * 为窗格中的每个元素调用 onElement 方法，方法的返回值决定了
	 * 是否窗格被执行来输出结果
	 */
	public abstract TriggerResult onElement(T element, long timestamp, W window, TriggerContext ctx) throws Exception;

	/**
	 * Called when a processing-time timer that was set using the trigger context fires.
	 *
	 * @param time The timestamp at which the timer fired.
	 * @param window The window for which the timer fired.
	 * @param ctx A context object that can be used to register timer callbacks.
	 */
	/**
	 * 使用 ctx 创建一个进程时间定时器，定时器触发的时候调用
	 */
	public abstract TriggerResult onProcessingTime(long time, W window, TriggerContext ctx) throws Exception;

	/**
	 * Called when an event-time timer that was set using the trigger context fires.
	 *
	 * @param time The timestamp at which the timer fired.
	 * @param window The window for which the timer fired.
	 * @param ctx A context object that can be used to register timer callbacks.
	 */
	/**
	 * 使用 ctx 来创建事件时间定时器，定时器触发的时候调用
	 */
	public abstract TriggerResult onEventTime(long time, W window, TriggerContext ctx) throws Exception;

	/**
	 * Returns true if this trigger supports merging of trigger state and can therefore
	 * be used with a
	 * {@link org.apache.flink.streaming.api.windowing.assigners.MergingWindowAssigner}.
	 *
	 * <p>If this returns {@code true} you must properly implement
	 * {@link #onMerge(Window, OnMergeContext)}
	 */
	/**
	 * 当 trigger 支持 trigger 状态合并的时候返回 true
	 * 返回 true 代表可以与 MergingWindowAssigner 一起使用
	 * 同时也需要实现 onMerge 方法
	 */
	public boolean canMerge() {
		return false;
	}

	/**
	 * Called when several windows have been merged into one window by the
	 * {@link org.apache.flink.streaming.api.windowing.assigners.WindowAssigner}.
	 *
	 * @param window The new window that results from the merge.
	 * @param ctx A context object that can be used to register timer callbacks and access state.
	 */
	/**
	 * 多个窗口被合并成一个窗口的时候调用
	 */
	public void onMerge(W window, OnMergeContext ctx) throws Exception {
		throw new UnsupportedOperationException("This trigger does not support merging.");
	}

	/**
	 * Clears any state that the trigger might still hold for the given window. This is called
	 * when a window is purged. Timers set using {@link TriggerContext#registerEventTimeTimer(long)}
	 * and {@link TriggerContext#registerProcessingTimeTimer(long)} should be deleted here as
	 * well as state acquired using {@link TriggerContext#getPartitionedState(StateDescriptor)}.
	 */
	/**
	 * 清除触发器可能仍为给定窗口保留的任何状态
	 * 当窗口被清除的时候调用这个方法
	 * 用 registerEventTimeTimer 或 registerProcessingTimeTimer 设置的定时器需要在这里被删除
	 * 用 getPartitionedState 获取的状态也需要被删除
	 */
	public abstract void clear(W window, TriggerContext ctx) throws Exception;

	// ------------------------------------------------------------------------

	/**
	 * A context object that is given to {@link Trigger} methods to allow them to register timer
	 * callbacks and deal with state.
	 */
	/**
	 * 一个 trigger 上下文
	 * trigger 可以使用 ctx 来注册定时器以及处理 state
	 */
	public interface TriggerContext {

		/**
		 * Returns the current processing time.
		 */
		/**
		 * 返回当前的进程时间
		 */
		long getCurrentProcessingTime();

		/**
		 * Returns the metric group for this {@link Trigger}. This is the same metric
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
		 * 返回当前的 watermark 时间
		 */
		long getCurrentWatermark();

		/**
		 * Register a system time callback. When the current system time passes the specified
		 * time {@link Trigger#onProcessingTime(long, Window, TriggerContext)} is called with the time specified here.
		 *
		 * @param time The time at which to invoke {@link Trigger#onProcessingTime(long, Window, TriggerContext)}
		 */
		/**
		 * 注册一个系统时间回调。如果当前的系统时间大于注册的时候
		 * onProcessingTime 被调用
		 */
		void registerProcessingTimeTimer(long time);

		/**
		 * Register an event-time callback. When the current watermark passes the specified
		 * time {@link Trigger#onEventTime(long, Window, TriggerContext)} is called with the time specified here.
		 *
		 * @param time The watermark at which to invoke {@link Trigger#onEventTime(long, Window, TriggerContext)}
		 * @see org.apache.flink.streaming.api.watermark.Watermark
		 */
		/**
		 * 注册一个事件时间回调。如果当前的事件时间大于注册的时候
		 * onEventTime 被调用
		 */
		void registerEventTimeTimer(long time);

		/**
		 * Delete the processing time trigger for the given time.
		 */
		/**
		 * 删除给定时间的进程时间触发器
		 */
		void deleteProcessingTimeTimer(long time);

		/**
		 * Delete the event-time trigger for the given time.
		 */
		/**
		 * 删除给定时间的事件事件触发器
		 */
		void deleteEventTimeTimer(long time);

		/**
		 * Retrieves a {@link State} object that can be used to interact with
		 * fault-tolerant state that is scoped to the window and key of the current
		 * trigger invocation.
		 *
		 * @param stateDescriptor The StateDescriptor that contains the name and type of the
		 *                        state that is being accessed.
		 * @param <S>             The type of the state.
		 * @return The partitioned state object.
		 * @throws UnsupportedOperationException Thrown, if no partitioned state is available for the
		 *                                       function (function is not part os a KeyedStream).
		 */
		/**
		 * 检索可用于与容错状态交互的状态对象，容错状态的作用域是当前触发器调用的窗口和键
		 */
		<S extends State> S getPartitionedState(StateDescriptor<S, ?> stateDescriptor);

		/**
		 * Retrieves a {@link ValueState} object that can be used to interact with
		 * fault-tolerant state that is scoped to the window and key of the current
		 * trigger invocation.
		 *
		 * @param name The name of the key/value state.
		 * @param stateType The class of the type that is stored in the state. Used to generate
		 *                  serializers for managed memory and checkpointing.
		 * @param defaultState The default state value, returned when the state is accessed and
		 *                     no value has yet been set for the key. May be null.
		 *
		 * @param <S>          The type of the state.
		 * @return The partitioned state object.
		 * @throws UnsupportedOperationException Thrown, if no partitioned state is available for the
		 *                                       function (function is not part os a KeyedStream).
		 * @deprecated Use {@link #getPartitionedState(StateDescriptor)}.
		 */
		@Deprecated
		<S extends Serializable> ValueState<S> getKeyValueState(String name, Class<S> stateType, S defaultState);


		/**
		 * Retrieves a {@link ValueState} object that can be used to interact with
		 * fault-tolerant state that is scoped to the window and key of the current
		 * trigger invocation.
		 *
		 * @param name The name of the key/value state.
		 * @param stateType The type information for the type that is stored in the state.
		 *                  Used to create serializers for managed memory and checkpoints.
		 * @param defaultState The default state value, returned when the state is accessed and
		 *                     no value has yet been set for the key. May be null.
		 *
		 * @param <S>          The type of the state.
		 * @return The partitioned state object.
		 * @throws UnsupportedOperationException Thrown, if no partitioned state is available for the
		 *                                       function (function is not part os a KeyedStream).
		 * @deprecated Use {@link #getPartitionedState(StateDescriptor)}.
		 */
		@Deprecated
		<S extends Serializable> ValueState<S> getKeyValueState(String name, TypeInformation<S> stateType, S defaultState);
	}

	/**
	 * Extension of {@link TriggerContext} that is given to
	 * {@link Trigger#onMerge(Window, OnMergeContext)}.
	 */
	/**
	 * TriggerContext 的扩展
	 * 在 onMerge 的时候使用
	 */
	public interface OnMergeContext extends TriggerContext {
		<S extends MergingState<?, ?>> void mergePartitionedState(StateDescriptor<S, ?> stateDescriptor);
	}
}
