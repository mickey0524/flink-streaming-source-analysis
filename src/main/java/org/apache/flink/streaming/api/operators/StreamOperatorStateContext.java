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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupStatePartitionStreamProvider;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.StatePartitionStreamProvider;
import org.apache.flink.util.CloseableIterable;

/**
 * This interface represents a context from which a stream operator can initialize everything connected to state such
 * as e.g. backends, raw state, and timer service manager.
 */
/**
 * 此接口表示一个上下文，流操作符可以从中初始化连接到状态的所有内容，例如 backend 、原始状态和计时器服务管理器
 */
public interface StreamOperatorStateContext {

	/**
	 * Returns true, the states provided by this context are restored from a checkpoint/savepoint.
	 */
	/**
	 * 返回 true，由这个上下文提供的 state 存储于 checkpoint/savepoint
	 */
	boolean isRestored();

	/**
	 * Returns the operator state backend for the stream operator.
	 */
	/**
	 * 返回操作符的状态 backend
	 */
	OperatorStateBackend operatorStateBackend();

	/**
	 * Returns the keyed state backend for the stream operator. This method returns null for non-keyed operators.
	 */
	/**
	 * 返回操作符的关键状态 backend
	 */
	AbstractKeyedStateBackend<?> keyedStateBackend();

	/**
	 * Returns the internal timer service manager for the stream operator. This method returns null for non-keyed
	 * operators.
	 */
	/**
	 * 返回操作符的内部时间管理器
	 */
	InternalTimeServiceManager<?> internalTimerServiceManager();

	/**
	 * Returns an iterable to obtain input streams for previously stored operator state partitions that are assigned to
	 * this stream operator.
	 */
	CloseableIterable<StatePartitionStreamProvider> rawOperatorStateInputs();

	/**
	 * Returns an iterable to obtain input streams for previously stored keyed state partitions that are assigned to
	 * this operator. This method returns null for non-keyed operators.
	 */
	CloseableIterable<KeyGroupStatePartitionStreamProvider> rawKeyedStateInputs();

}
