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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.streaming.api.checkpoint.ExternallyInducedSource;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.util.FlinkException;

/**
 * {@link StreamTask} for executing a {@link StreamSource}.
 * 执行 StreamSource 和 StreamTask
 *
 * <p>One important aspect of this is that the checkpointing and the emission of elements must never
 * occur at the same time. The execution must be serial. This is achieved by having the contract
 * with the {@link SourceFunction} that it must only modify its state or emit elements in
 * a synchronized block that locks on the lock Object. Also, the modification of the state
 * and the emission of elements must happen in the same block of code that is protected by the
 * synchronized block.
 *
 * 一个很重要的方面是检查点操作和元素的 emit 不能同时发送，执行一定要是串行的
 * 这个是通过加锁来实现的，同一，状态的更改和元素的 emit 也需要加锁互斥执行
 *
 * @param <OUT> Type of the output elements of this source.
 * @param <SRC> Type of the source function for the stream source operator
 * @param <OP> Type of the stream source operator
 */
@Internal
public class SourceStreamTask<OUT, SRC extends SourceFunction<OUT>, OP extends StreamSource<OUT, SRC>>
	extends StreamTask<OUT, OP> {

	private volatile boolean externallyInducedCheckpoints;

	public SourceStreamTask(Environment env) {
		super(env);
	}

	@Override
	protected void init() {
		// we check if the source is actually inducing the checkpoints, rather
		// than the trigger
		// 我们检查是否数据源在引发检查点，而不是触发器
		SourceFunction<?> source = headOperator.getUserFunction();
		if (source instanceof ExternallyInducedSource) {
			externallyInducedCheckpoints = true;  // 检查点由数据源函数触发

			ExternallyInducedSource.CheckpointTrigger triggerHook = new ExternallyInducedSource.CheckpointTrigger() {

				@Override
				public void triggerCheckpoint(long checkpointId) throws FlinkException {
					// TODO - we need to see how to derive those. We should probably not encode this in the
					// TODO -   source's trigger message, but do a handshake in this task between the trigger
					// TODO -   message from the master, and the source's trigger notification
					final CheckpointOptions checkpointOptions = CheckpointOptions.forCheckpointWithDefaultLocation();
					final long timestamp = System.currentTimeMillis();
					
					// 检查点元数据，包含检查点 id 和 ts
					final CheckpointMetaData checkpointMetaData = new CheckpointMetaData(checkpointId, timestamp);

					try {
						SourceStreamTask.super.triggerCheckpoint(checkpointMetaData, checkpointOptions);
					}
					catch (RuntimeException | FlinkException e) {
						throw e;
					}
					catch (Exception e) {
						throw new FlinkException(e.getMessage(), e);
					}
				}
			};

			((ExternallyInducedSource<?, ?>) source).setCheckpointTrigger(triggerHook);
		}
	}

	@Override
	protected void cleanup() {
		// does not hold any resources, so no cleanup needed
	}

	@Override
	// 重写父类的 run 方法，run 方法会在 StreamTask 的 invoke 方法里被调用
	// 父类的 invoke 方法会被 TaskManager 执行
	protected void run() throws Exception {
		// chain 的头部操作符 run 起来
		headOperator.run(getCheckpointLock(), getStreamStatusMaintainer());
	}

	@Override
	protected void cancelTask() throws Exception {
		if (headOperator != null) {
			headOperator.cancel();
		}
	}

	// ------------------------------------------------------------------------
	//  Checkpointing
	// ------------------------------------------------------------------------

	@Override
	public boolean triggerCheckpoint(CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions) throws Exception {
		if (!externallyInducedCheckpoints) {
			return super.triggerCheckpoint(checkpointMetaData, checkpointOptions);
		}
		else {
			// we do not trigger checkpoints here, we simply state whether we can trigger them
			// 我们不会在这里触发检查点，我们只是说明我们是否可以触发它们
			synchronized (getCheckpointLock()) {
				return isRunning();
			}
		}
	}
}
