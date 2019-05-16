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

import org.apache.flink.util.ExceptionUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * This class contains the combined results from the snapshot of a state backend:
 * <ul>
 *   <li>A state object representing the state that will be reported to the Job Manager to acknowledge the checkpoint.</li>
 *   <li>A state object that represents the state for the {@link TaskLocalStateStoreImpl}.</li>
 * </ul>
 *
 * Both state objects are optional and can be null, e.g. if there was no state to snapshot in the backend. A local
 * state object that is not null also requires a state to report to the job manager that is not null, because the
 * Job Manager always owns the ground truth about the checkpointed state.
 */
/**
 * SnapshotResult 包含状态 backend 快照的组合结果:
 * 1. 一个状态对象，表示将报告给作业管理器以确认检查点的状态
 * 2. 一个状态对象，表示TaskLocalStateStoreImpl的状态
 * 
 * 两个状态对象都是可选的，可以为 null。例如，backend 没有状态快照
 * 非 null 的本地状态对象还需要状态向作业管理器报告非空，因为作业管理器始终拥有关于检查点状态的基本事实
 */
public class SnapshotResult<T extends StateObject> implements StateObject {

	private static final long serialVersionUID = 1L;

	/** An singleton instance to represent an empty snapshot result. */
	// 单例表示一个空的 snapshot result
	private static final SnapshotResult<?> EMPTY = new SnapshotResult<>(null, null);

	/** This is the state snapshot that will be reported to the Job Manager to acknowledge a checkpoint. */
	// 这是将报告给作业管理器以确认检查点的状态快照
	private final T jobManagerOwnedSnapshot;

	/** This is the state snapshot that will be reported to the Job Manager to acknowledge a checkpoint. */
	// 这是将报告给作业管理器以确认检查点的状态快照
	private final T taskLocalSnapshot;

	/**
	 * Creates a {@link SnapshotResult} for the given jobManagerOwnedSnapshot and taskLocalSnapshot. If the
	 * jobManagerOwnedSnapshot is null, taskLocalSnapshot must also be null.
	 *
	 * @param jobManagerOwnedSnapshot Snapshot for report to job manager. Can be null.
	 * @param taskLocalSnapshot Snapshot for report to local state manager. This is optional and requires
	 *                             jobManagerOwnedSnapshot to be not null if this is not also null.
	 */
	/**
	 * 创建一个 SnapshotResult
	 * 如果 jobManagerOwnedSnapshot 为空，taskLocalSnapshot 一定为空
	 */
	private SnapshotResult(T jobManagerOwnedSnapshot, T taskLocalSnapshot) {

		if (jobManagerOwnedSnapshot == null && taskLocalSnapshot != null) {
			throw new IllegalStateException("Cannot report local state snapshot without corresponding remote state!");
		}

		this.jobManagerOwnedSnapshot = jobManagerOwnedSnapshot;
		this.taskLocalSnapshot = taskLocalSnapshot;
	}

	public T getJobManagerOwnedSnapshot() {
		return jobManagerOwnedSnapshot;
	}

	public T getTaskLocalSnapshot() {
		return taskLocalSnapshot;
	}

	/**
	 * 丢弃状态
	 */
	@Override
	public void discardState() throws Exception {

		Exception aggregatedExceptions = null;

		if (jobManagerOwnedSnapshot != null) {
			try {
				jobManagerOwnedSnapshot.discardState();
			} catch (Exception remoteDiscardEx) {
				aggregatedExceptions = remoteDiscardEx;
			}
		}

		if (taskLocalSnapshot != null) {
			try {
				taskLocalSnapshot.discardState();
			} catch (Exception localDiscardEx) {
				aggregatedExceptions = ExceptionUtils.firstOrSuppressed(localDiscardEx, aggregatedExceptions);
			}
		}

		if (aggregatedExceptions != null) {
			throw aggregatedExceptions;
		}
	}

	@Override
	public long getStateSize() {
		return jobManagerOwnedSnapshot != null ? jobManagerOwnedSnapshot.getStateSize() : 0L;
	}

	@SuppressWarnings("unchecked")
	public static <T extends StateObject> SnapshotResult<T> empty() {
		return (SnapshotResult<T>) EMPTY;
	}

	// 静态工厂函数
	public static <T extends StateObject> SnapshotResult<T> of(@Nullable T jobManagerState) {
		return jobManagerState != null ? new SnapshotResult<>(jobManagerState, null) : empty();
	}

	public static <T extends StateObject> SnapshotResult<T> withLocalState(
		@Nonnull T jobManagerState,
		@Nonnull T localState) {
		return new SnapshotResult<>(jobManagerState, localState);
	}
}
