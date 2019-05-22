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

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.state.OperatorStateStore;

/**
 * Default implementation of {@link StateInitializationContext}.
 */
/**
 * StateInitializationContext 的默认实现
 */
public class StateInitializationContextImpl implements StateInitializationContext {

	/** Signal whether any state to restore was found */
	// 是否存在要恢复的状态的信号
	private final boolean restored;

	private final OperatorStateStore operatorStateStore;

	private final KeyedStateStore keyedStateStore;

	private final Iterable<KeyGroupStatePartitionStreamProvider> rawKeyedStateInputs;
	private final Iterable<StatePartitionStreamProvider> rawOperatorStateInputs;

	public StateInitializationContextImpl(
			boolean restored,
			OperatorStateStore operatorStateStore,
			KeyedStateStore keyedStateStore,
			Iterable<KeyGroupStatePartitionStreamProvider> rawKeyedStateInputs,
			Iterable<StatePartitionStreamProvider> rawOperatorStateInputs) {

		this.restored = restored;  // 标识我们是第一次启动还是恢复
		this.operatorStateStore = operatorStateStore;  // 操作符状态存储
		this.keyedStateStore = keyedStateStore;  // keyed 状态存储
		this.rawOperatorStateInputs = rawOperatorStateInputs;  // 连接 keyed 状态流
		this.rawKeyedStateInputs = rawKeyedStateInputs;  // 连接操作符状态流
	}

	@Override
	public boolean isRestored() {
		return restored;
	}

	@Override
	public Iterable<StatePartitionStreamProvider> getRawOperatorStateInputs() {
		return rawOperatorStateInputs;
	}

	@Override
	public Iterable<KeyGroupStatePartitionStreamProvider> getRawKeyedStateInputs() {
		if(null == keyedStateStore) {
			throw new IllegalStateException("Attempt to access keyed state from non-keyed operator.");
		}

		return rawKeyedStateInputs;
	}

	@Override
	public OperatorStateStore getOperatorStateStore() {
		return operatorStateStore;
	}

	@Override
	public KeyedStateStore getKeyedStateStore() {
		return keyedStateStore;
	}
}
