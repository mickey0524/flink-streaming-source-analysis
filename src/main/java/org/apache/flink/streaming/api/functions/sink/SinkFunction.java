/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions.sink;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.functions.Function;

import java.io.Serializable;

/**
 * Interface for implementing user defined sink functionality.
 *
 * @param <IN> Input type parameter.
 */
/**
 * 实现用户定义的 sink 函数的接口
 */
@Public
public interface SinkFunction<IN> extends Function, Serializable {

	/**
	 * @deprecated Use {@link #invoke(Object, Context)}.
	 */
	@Deprecated
	default void invoke(IN value) throws Exception {}

	/**
	 * Writes the given value to the sink. This function is called for every record.
	 *
	 * <p>You have to override this method when implementing a {@code SinkFunction}, this is a
	 * {@code default} method for backward compatibility with the old-style method only.
	 *
	 * @param value The input record.
	 * @param context Additional context about the input record.
	 *
	 * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation
	 *                   to fail and may trigger recovery.
	 */
	/**
	 * 将 value sink 到下游，每一个 StreamRecord 都需要调用这个函数
	 * 当你实现一个 SinkFunction 的时候，你需要重写这个方法，default 是为了兼容老版本
	 */
	default void invoke(IN value, Context context) throws Exception {
		invoke(value);
	}

	/**
	 * Context that {@link SinkFunction SinkFunctions } can use for getting additional data about
	 * an input record.
	 *
	 * <p>The context is only valid for the duration of a
	 * {@link SinkFunction#invoke(Object, Context)} call. Do not store the context and use
	 * afterwards!
	 *
	 * @param <T> The type of elements accepted by the sink.
	 */
	/**
	 * SinkFunction 可以从 Context 中获取输入 record 的其他信息
	 * SinkFunction 仅仅在 invoke 函数内是有效的，不要存储上下文
	 */
	@Public // Interface might be extended in the future with additional methods.
	interface Context<T> {

		/** Returns the current processing time. */
		/**
		 * 获取当前的进程时间
		 */
		long currentProcessingTime();

		/** Returns the current event-time watermark. */
		/**
		 * 获取当前的 event-time watermark
		 */
		long currentWatermark();

		/**
		 * Returns the timestamp of the current input record or {@code null} if the element does not
		 * have an assigned timestamp.
		 */
		/**
		 * 获取当前输入 input 的 ts
		 */
		Long timestamp();
	}
}
