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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.RunnableFuture;

/**
 * This class is a default implementation for StateSnapshotContext.
 */
/**
 * StateSnapshotContext 的默认实现
 */
public class StateSnapshotContextSynchronousImpl implements StateSnapshotContext, Closeable {

	/** Checkpoint id of the snapshot. */
	// 快照的检查点 id
	private final long checkpointId;

	/** Checkpoint timestamp of the snapshot. */
	// 快照的检查点 ts
	private final long checkpointTimestamp;
	
	/** Factory for he checkpointing stream */
	// 检查点流的工厂
	private final CheckpointStreamFactory streamFactory;
	
	/** Key group range for the operator that created this context. Only for keyed operators */
	// 创建本上下文的操作符的 Key group range
	private final KeyGroupRange keyGroupRange;

	/**
	 * Registry for opened streams to participate in the lifecycle of the stream task. Hence, this registry should be 
	 * obtained from and managed by the stream task.
	 */
	// 用于打开流的注册表，以参与流任务的生命周期。因此，此注册表应从流任务获取并由其管理
	private final CloseableRegistry closableRegistry;

	/** Output stream for the raw keyed state. */
	// raw keyed state 的输出流
	private KeyedStateCheckpointOutputStream keyedStateCheckpointOutputStream;

	/** Output stream for the raw operator state. */
	// raw operator state 的输出流
	private OperatorStateCheckpointOutputStream operatorStateCheckpointOutputStream;

	@VisibleForTesting
	public StateSnapshotContextSynchronousImpl(long checkpointId, long checkpointTimestamp) {
		this.checkpointId = checkpointId;
		this.checkpointTimestamp = checkpointTimestamp;
		this.streamFactory = null;
		this.keyGroupRange = KeyGroupRange.EMPTY_KEY_GROUP_RANGE;
		this.closableRegistry = null;
	}


	public StateSnapshotContextSynchronousImpl(
			long checkpointId,
			long checkpointTimestamp,
			CheckpointStreamFactory streamFactory,
			KeyGroupRange keyGroupRange,
			CloseableRegistry closableRegistry) {

		this.checkpointId = checkpointId;
		this.checkpointTimestamp = checkpointTimestamp;
		this.streamFactory = Preconditions.checkNotNull(streamFactory);
		this.keyGroupRange = Preconditions.checkNotNull(keyGroupRange);
		this.closableRegistry = Preconditions.checkNotNull(closableRegistry);
	}

	/**
	 * 获取检查点 id
	 */
	@Override
	public long getCheckpointId() {
		return checkpointId;
	}

	/**
	 * 获取检查点时间戳
	 */
	@Override
	public long getCheckpointTimestamp() {
		return checkpointTimestamp;
	}

	/**
	 * 打开一个新的流，并在 closableRegistry 中注册
	 */
	private CheckpointStreamFactory.CheckpointStateOutputStream openAndRegisterNewStream() throws Exception {
		CheckpointStreamFactory.CheckpointStateOutputStream cout =
				streamFactory.createCheckpointStateOutputStream(CheckpointedStateScope.EXCLUSIVE);

		closableRegistry.registerCloseable(cout);
		return cout;
	}

	/**
	 * 创建用于 KeyedState 的检查点输出流
	 */
	@Override
	public KeyedStateCheckpointOutputStream getRawKeyedOperatorStateOutput() throws Exception {
		if (null == keyedStateCheckpointOutputStream) {
			// 只有在 keyed 操作符能申请 KeyedState 的检查点输出流
			Preconditions.checkState(keyGroupRange != KeyGroupRange.EMPTY_KEY_GROUP_RANGE, "Not a keyed operator");
			keyedStateCheckpointOutputStream = new KeyedStateCheckpointOutputStream(openAndRegisterNewStream(), keyGroupRange);
		}
		return keyedStateCheckpointOutputStream;
	}

	/**
	 * 创建用于 OperatorState 的检查点输出流
	 */
	@Override
	public OperatorStateCheckpointOutputStream getRawOperatorStateOutput() throws Exception {
		if (null == operatorStateCheckpointOutputStream) {
			operatorStateCheckpointOutputStream = new OperatorStateCheckpointOutputStream(openAndRegisterNewStream());
		}
		return operatorStateCheckpointOutputStream;
	}

	/**
	 * 用于获取 KeyedState 的 Future
	 */
	@Nonnull
	public RunnableFuture<SnapshotResult<KeyedStateHandle>> getKeyedStateStreamFuture() throws IOException {
		KeyedStateHandle keyGroupsStateHandle =
			closeAndUnregisterStreamToObtainStateHandle(keyedStateCheckpointOutputStream);
		return toDoneFutureOfSnapshotResult(keyGroupsStateHandle);
	}

	/**
	 * 用于获取 OperatorState 的 Future
	 */
	@Nonnull
	public RunnableFuture<SnapshotResult<OperatorStateHandle>> getOperatorStateStreamFuture() throws IOException {
		OperatorStateHandle operatorStateHandle =
			closeAndUnregisterStreamToObtainStateHandle(operatorStateCheckpointOutputStream);
		return toDoneFutureOfSnapshotResult(operatorStateHandle);
	}

	private <T extends StateObject> RunnableFuture<SnapshotResult<T>> toDoneFutureOfSnapshotResult(T handle) {
		SnapshotResult<T> snapshotResult = SnapshotResult.of(handle);
		return DoneFuture.of(snapshotResult);
	}

	// 成功关闭流，获取 StreamStateHandle，同样需要解除注册
	private <T extends StreamStateHandle> T closeAndUnregisterStreamToObtainStateHandle(
		NonClosingCheckpointOutputStream<T> stream) throws IOException {

		if (stream != null && closableRegistry.unregisterCloseable(stream.getDelegate())) {
			return stream.closeAndGetHandle();
		} else {
			return null;
		}
	}

	// 关闭输出流，同时解除注册
	private <T extends StreamStateHandle> void closeAndUnregisterStream(
		NonClosingCheckpointOutputStream<? extends T> stream) throws IOException {

		Preconditions.checkNotNull(stream);

		CheckpointStreamFactory.CheckpointStateOutputStream delegate = stream.getDelegate();
		
		// 从关闭注册表中删除，然后关闭流
		if (closableRegistry.unregisterCloseable(delegate)) {
			delegate.close();
		}
	}

	@Override
	// 关闭 StateSnapshotContext
	public void close() throws IOException {
		IOException exception = null;

		if (keyedStateCheckpointOutputStream != null) {
			try {
				// 关闭 keyedStateCheckpointOutputStream
				closeAndUnregisterStream(keyedStateCheckpointOutputStream);
			} catch (IOException e) {
				exception = new IOException("Could not close the raw keyed state checkpoint output stream.", e);
			}
		}

		if (operatorStateCheckpointOutputStream != null) {
			try {
				// 关闭 operatorStateCheckpointOutputStream
				closeAndUnregisterStream(operatorStateCheckpointOutputStream);
			} catch (IOException e) {
				exception = ExceptionUtils.firstOrSuppressed(
					new IOException("Could not close the raw operator state checkpoint output stream.", e),
					exception);
			}
		}

		if (exception != null) {
			throw exception;
		}
	}
}
