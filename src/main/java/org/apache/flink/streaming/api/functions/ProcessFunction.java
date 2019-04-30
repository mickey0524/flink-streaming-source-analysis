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

package org.apache.flink.streaming.api.functions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * A function that processes elements of a stream.
 * 一个在流中处理元素的函数
 *
 * <p>For every element in the input stream {@link #processElement(Object, Context, Collector)}
 * is invoked. This can produce zero or more elements as output. Implementations can also
 * query the time and set timers through the provided {@link Context}. For firing timers
 * {@link #onTimer(long, OnTimerContext, Collector)} will be invoked. This can again produce
 * zero or more elements as output and register further timers.
 *
 * 对于输入流中的每一个元素，调用 processElement 方法
 * 这个可能产生一个或者多个元素作为输出
 * 实例可以通过 Context 请求时间或者设置定时器
 * 定时器触发的时候，onTimer 会被调用
 * 这也能产生一个或多个元素作为输出，而且可以注册更多定时器
 *
 * <p><b>NOTE:</b> Access to keyed state and timers (which are also scoped to a key) is only
 * available if the {@code ProcessFunction} is applied on a {@code KeyedStream}.
 *
 * <p><b>NOTE:</b> A {@code ProcessFunction} is always a
 * {@link org.apache.flink.api.common.functions.RichFunction}. Therefore, access to the
 * {@link org.apache.flink.api.common.functions.RuntimeContext} is always available and setup and
 * teardown methods can be implemented. See
 * {@link org.apache.flink.api.common.functions.RichFunction#open(org.apache.flink.configuration.Configuration)}
 * and {@link org.apache.flink.api.common.functions.RichFunction#close()}.
 *
 * @param <I> Type of the input elements.
 * @param <O> Type of the output elements.
 */
/**
 * ProcessFunction 是一个低级流处理算子操作，可以访问所有（非循环）流应用程序的基本构建块
 * 1. 事件（流数据元）
 * 2. state（容错，一致，仅在被Key化的数据流上）
 * 3. 定时器（事件时间和处理时间，仅限被Key化的数据流）
 * ProcessFunction 可被认为是一个可以访问 Keys 状态和定时器的 FlatMapFunction
 * 它通过为输入流中接收的每个事件调用来处理事件
 * 对于容错状态，ProcessFunction 可以访问 Flink 的被 Keys 化状态，可以通过其访问 RuntimeContext
 * 类似于其他有状态函数可以访问被 Keys 化状态的方式
 * 定时器允许应用程序对处理时间和事件时间的变化作出反应。每次调用该函数 processElement(...) 都会获得一个Context对象
 * 该对象可以访问数据元的事件时间戳和 TimerService
 * TimerService 可用于注册为将来事件- /处理-时刻回调。达到计时器的特定时间时，onTimer(...) 将调用该方法
 * 在该调用期间，所有状态再次限定为创建计时器的键，允许计时器操纵被 Keys 化状态
 * 如果要访问被 Keys 化状态和计时器，则必须应用 ProcessFunction 被 Key 化的数据流
 */
@PublicEvolving
public abstract class ProcessFunction<I, O> extends AbstractRichFunction {

	private static final long serialVersionUID = 1L;

	/**
	 * Process one element from the input stream.
	 *
	 * <p>This function can output zero or more elements using the {@link Collector} parameter
	 * and also update internal state or set timers using the {@link Context} parameter.
	 *
	 * @param value The input value.
	 * @param ctx A {@link Context} that allows querying the timestamp of the element and getting
	 *            a {@link TimerService} for registering timers and querying the time. The
	 *            context is only valid during the invocation of this method, do not store it.
	 * @param out The collector for returning result values.
	 *
	 * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation
	 *                   to fail and may trigger recovery.
	 */
	/**
	 * 处理输入流的一个元素
	 * 这个函数能够使用 Collector 输出一个或多个元素
	 * 也能使用 Context 更新内部状态和设置定时器
	 */
	public abstract void processElement(I value, Context ctx, Collector<O> out) throws Exception;

	/**
	 * Called when a timer set using {@link TimerService} fires.
	 *
	 * @param timestamp The timestamp of the firing timer.
	 * @param ctx An {@link OnTimerContext} that allows querying the timestamp of the firing timer,
	 *            querying the {@link TimeDomain} of the firing timer and getting a
	 *            {@link TimerService} for registering timers and querying the time.
	 *            The context is only valid during the invocation of this method, do not store it.
	 * @param out The collector for returning result values.
	 *
	 * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation
	 *                   to fail and may trigger recovery.
	 */
	/**
	 * 当定时器被触发的时候调用
	 */
	public void onTimer(long timestamp, OnTimerContext ctx, Collector<O> out) throws Exception {}

	/**
	 * Information available in an invocation of {@link #processElement(Object, Context, Collector)}
	 * or {@link #onTimer(long, OnTimerContext, Collector)}.
	 */
	public abstract class Context {

		/**
		 * Timestamp of the element currently being processed or timestamp of a firing timer.
		 *
		 * <p>This might be {@code null}, for example if the time characteristic of your program
		 * is set to {@link org.apache.flink.streaming.api.TimeCharacteristic#ProcessingTime}.
		 */
		/**
		 * 当前被处理的元素的 ts 或者被触发的定时器的 ts
		 * 函数有可能返回 null，例如，当 time characteristic 设置为 ProcessingTime
		 */
		public abstract Long timestamp();

		/**
		 * A {@link TimerService} for querying time and registering timers.
		 */
		/**
		 * 一个请求时间和注册定时器的时间服务
		 */
		public abstract TimerService timerService();

		/**
		 * Emits a record to the side output identified by the {@link OutputTag}.
		 *
		 * @param outputTag the {@code OutputTag} that identifies the side output to emit to.
		 * @param value The record to emit.
		 */
		/**
		 * 侧边输出记录
		 */
		public abstract <X> void output(OutputTag<X> outputTag, X value);
	}

	/**
	 * Information available in an invocation of {@link #onTimer(long, OnTimerContext, Collector)}.
	 */
	public abstract class OnTimerContext extends Context {
		/**
		 * The {@link TimeDomain} of the firing timer.
		 */
		/**
		 * 定时器依赖的时间域
		 */
		public abstract TimeDomain timeDomain();
	}

}
