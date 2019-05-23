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

package org.apache.flink.streaming.api.functions.async;

import org.apache.flink.annotation.PublicEvolving;

import java.util.Collection;

/**
 * {@link ResultFuture} collects data / error in user codes while processing async i/o.
 *
 * @param <OUT> Output type
 */
/**
 * ResultFuture 在处理异步 i/o 时收集用户代码中的数据/错误
 */
@PublicEvolving
public interface ResultFuture<OUT> {
	/**
	 * Completes the result future with a collection of result objects.
	 *
	 * <p>Note that it should be called for exactly one time in the user code.
	 * Calling this function for multiple times will cause data lose.
	 *
	 * <p>Put all results in a {@link Collection} and then emit output.
	 *
	 * @param result A list of results.
	 */
	/**
	 * 使用结果对象集合完成结果
	 * 
	 * 请注意，应该在用户代码中只调用一次
	 * 多次调用此函数将导致数据丢失
	 * 
	 * 将所有结果放入 Collection 中，然后 emit 输出
	 */
	void complete(Collection<OUT> result);

	/**
	 * Completes the result future exceptionally with an exception.
	 *
	 * @param error A Throwable object.
	 */
	/**
	 * 发生异常
	 */
	void completeExceptionally(Throwable error);
}
