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

import org.apache.flink.runtime.state.DoneFuture;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateUtil;
import org.apache.flink.util.ExceptionUtils;

import javax.annotation.Nonnull;

import java.util.concurrent.RunnableFuture;

/**
 * Result of {@link StreamOperator#snapshotState}.
 */
/**
 * StreamOperator.snapshotState 的结果
 * 
 * http://flink-cn.shinonomelab.com/dev/stream/state/state.html 4 类 state
 * rawState 就是一堆字节，flink 并不知道
 * manageState 是 flink 知道的，ListState，ReduceState 这样的，推荐使用 manageState
 * flink 能够自动重新分配 manageState 当并行度改变的时候
 * 
 * keyed manage state 只能用于 KeyedStream
 * Operator manage state 没有这个限制，function 需要实现 CheckpointedFunction or ListCheckpointed
 */
public class OperatorSnapshotFutures {

	@Nonnull
	private RunnableFuture<SnapshotResult<KeyedStateHandle>> keyedStateManagedFuture;

	@Nonnull
	private RunnableFuture<SnapshotResult<KeyedStateHandle>> keyedStateRawFuture;

	@Nonnull
	private RunnableFuture<SnapshotResult<OperatorStateHandle>> operatorStateManagedFuture;

	@Nonnull
	private RunnableFuture<SnapshotResult<OperatorStateHandle>> operatorStateRawFuture;

	public OperatorSnapshotFutures() {
		this(
			DoneFuture.of(SnapshotResult.empty()),
			DoneFuture.of(SnapshotResult.empty()),
			DoneFuture.of(SnapshotResult.empty()),
			DoneFuture.of(SnapshotResult.empty()));
	}

	public OperatorSnapshotFutures(
		@Nonnull RunnableFuture<SnapshotResult<KeyedStateHandle>> keyedStateManagedFuture,
		@Nonnull RunnableFuture<SnapshotResult<KeyedStateHandle>> keyedStateRawFuture,
		@Nonnull RunnableFuture<SnapshotResult<OperatorStateHandle>> operatorStateManagedFuture,
		@Nonnull RunnableFuture<SnapshotResult<OperatorStateHandle>> operatorStateRawFuture) {
		this.keyedStateManagedFuture = keyedStateManagedFuture;
		this.keyedStateRawFuture = keyedStateRawFuture;
		this.operatorStateManagedFuture = operatorStateManagedFuture;
		this.operatorStateRawFuture = operatorStateRawFuture;
	}

	@Nonnull
	public RunnableFuture<SnapshotResult<KeyedStateHandle>> getKeyedStateManagedFuture() {
		return keyedStateManagedFuture;
	}

	public void setKeyedStateManagedFuture(
		@Nonnull RunnableFuture<SnapshotResult<KeyedStateHandle>> keyedStateManagedFuture) {
		this.keyedStateManagedFuture = keyedStateManagedFuture;
	}

	@Nonnull
	public RunnableFuture<SnapshotResult<KeyedStateHandle>> getKeyedStateRawFuture() {
		return keyedStateRawFuture;
	}

	public void setKeyedStateRawFuture(
		@Nonnull RunnableFuture<SnapshotResult<KeyedStateHandle>> keyedStateRawFuture) {
		this.keyedStateRawFuture = keyedStateRawFuture;
	}

	@Nonnull
	public RunnableFuture<SnapshotResult<OperatorStateHandle>> getOperatorStateManagedFuture() {
		return operatorStateManagedFuture;
	}

	public void setOperatorStateManagedFuture(
		@Nonnull RunnableFuture<SnapshotResult<OperatorStateHandle>> operatorStateManagedFuture) {
		this.operatorStateManagedFuture = operatorStateManagedFuture;
	}

	@Nonnull
	public RunnableFuture<SnapshotResult<OperatorStateHandle>> getOperatorStateRawFuture() {
		return operatorStateRawFuture;
	}

	public void setOperatorStateRawFuture(
		@Nonnull RunnableFuture<SnapshotResult<OperatorStateHandle>> operatorStateRawFuture) {
		this.operatorStateRawFuture = operatorStateRawFuture;
	}

	// cancel 的时候，需要将 Future 对象全部 cancel
	public void cancel() throws Exception {
		Exception exception = null;

		try {
			StateUtil.discardStateFuture(getKeyedStateManagedFuture());
		} catch (Exception e) {
			exception = new Exception("Could not properly cancel managed keyed state future.", e);
		}

		try {
			StateUtil.discardStateFuture(getOperatorStateManagedFuture());
		} catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(
				new Exception("Could not properly cancel managed operator state future.", e),
				exception);
		}

		try {
			StateUtil.discardStateFuture(getKeyedStateRawFuture());
		} catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(
				new Exception("Could not properly cancel raw keyed state future.", e),
				exception);
		}

		try {
			StateUtil.discardStateFuture(getOperatorStateRawFuture());
		} catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(
				new Exception("Could not properly cancel raw operator state future.", e),
				exception);
		}

		if (exception != null) {
			throw exception;
		}
	}
}
