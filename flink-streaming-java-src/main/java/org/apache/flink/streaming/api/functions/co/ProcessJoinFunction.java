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

package org.apache.flink.streaming.api.functions.co;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * A function that processes two joined elements and produces a single output one.
 *
 * <p>This function will get called for every joined pair of elements the joined two streams.
 * The timestamp of the joined pair as well as the timestamp of the left element and the right
 * element can be accessed through the {@link Context}.
 *
 * @param <IN1> Type of the first input
 * @param <IN2> Type of the second input
 * @param <OUT> Type of the output
 */
/**
 * 一个处理两个连接元素并生成单个输出元素的函数
 *
 * 对于连接的两个流的每个连接元素对，将调用此函数
 * 可以通过Context访问连接对的时间戳以及左侧元素和右侧元素的时间戳
 */
@PublicEvolving
public abstract class ProcessJoinFunction<IN1, IN2, OUT> extends AbstractRichFunction {

	private static final long serialVersionUID = -2444626938039012398L;

	/**
	 * This method is called for each joined pair of elements. It can output zero or more elements
	 * through the provided {@link Collector} and has access to the timestamps of the joined elements
	 * and the result through the {@link Context}.
	 *
	 * @param left         The left element of the joined pair.
	 * @param right        The right element of the joined pair.
	 * @param ctx          A context that allows querying the timestamps of the left, right and
	 *                     joined pair. In addition, this context allows to emit elements on a side output.
	 * @param out          The collector to emit resulting elements to.
	 * @throws Exception   This function may throw exceptions which cause the streaming program to
	 * 					   fail and go in recovery mode.
	 */
	/**
	 * 每个连接对都会调用这个方法
	 * 这个方法通过给定的 Collector 输出零个或者多个元素
	 * 通过可以通过 Context 来访问连接元素的时间戳
	 */
	public abstract void processElement(IN1 left, IN2 right, Context ctx, Collector<OUT> out) throws Exception;

	/**
	 * The context that is available during an invocation of
	 * {@link #processElement(Object, Object, Context, Collector)}. It gives access to the timestamps of the
	 * left element in the joined pair, the right one, and that of the joined pair. In addition, this context
	 * allows to emit elements on a side output.
	 */
	/**
	 * 在 processElement 方法执行的时候，Context 可以被使用
	 * Context 能够访问连接对左端元素的时间戳，右端元素的时间戳
	 * 以及连接对的时间戳（join pair 中更大的 ts），此外，Context 允许偏侧输出
	 */
	public abstract class Context {

		/**
		 * @return The timestamp of the left element of a joined pair
		 */
		/**
		 * 返回 join 对中左边元素的 ts
		 */
		public abstract long getLeftTimestamp();

		/**
		 * @return The timestamp of the right element of a joined pair
		 */
		/**
		 * 返回 join 对中右边元素的 ts
		 */
		public abstract long getRightTimestamp();

		/**
		 * @return The timestamp of the joined pair.
		 */
		/**
		 * 返回 join 对的 ts
		 */
		public abstract long getTimestamp();

		/**
		 * Emits a record to the side output identified by the {@link OutputTag}.
		 * @param outputTag The output tag that identifies the side output to emit to
		 * @param value The record to emit
		 */
		/**
		 * 提供侧边输出的接口
		 */
		public abstract <X> void output(OutputTag<X> outputTag, X value);
	}
}
