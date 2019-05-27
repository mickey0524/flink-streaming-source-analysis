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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.util.Preconditions;

/**
 * This factory produces {@link CheckpointExceptionHandler} instances that handle exceptions during checkpointing in a
 * {@link StreamTask}.
 */
/**
 * 此工厂生成 CheckpointExceptionHandler 实例，用于在 StreamTask 中检查点期间处理异常
 */
public class CheckpointExceptionHandlerFactory {

	/**
	 * Returns a {@link CheckpointExceptionHandler} that either causes a task to fail completely or to just declines
	 * checkpoint on exception, depending on the parameter flag.
	 */
	/**
	 * 返回一个 CheckpointExceptionHandler，它会导致任务完全失败
	 * 或者只是在异常时拒绝检查点，具体取决于参数标志
	 */
	public CheckpointExceptionHandler createCheckpointExceptionHandler(
		boolean failTaskOnCheckpointException,
		Environment environment) {

		if (failTaskOnCheckpointException) {
			return new FailingCheckpointExceptionHandler();
		} else {
			return new DecliningCheckpointExceptionHandler(environment);
		}
	}

	/**
	 * This handler makes the task fail by rethrowing a reported exception.
	 */
	/**
	 * 此处理程序通过重新抛出报告的异常使任务失败
	 */
	static final class FailingCheckpointExceptionHandler implements CheckpointExceptionHandler {

		@Override
		public void tryHandleCheckpointException(
			CheckpointMetaData checkpointMetaData,
			Exception exception) throws Exception {

			throw exception;
		}
	}

	/**
	 * This handler makes the task decline the checkpoint as reaction to the reported exception. The task is not failed.
	 */
	/**
	 * 此处理程序使任务拒绝检查点作为对报告的异常的反应，任务没有失败
	 */
	static final class DecliningCheckpointExceptionHandler implements CheckpointExceptionHandler {

		final Environment environment;

		DecliningCheckpointExceptionHandler(Environment environment) {
			this.environment = Preconditions.checkNotNull(environment);
		}

		@Override
		public void tryHandleCheckpointException(
			CheckpointMetaData checkpointMetaData,
			Exception exception) throws Exception {

			environment.declineCheckpoint(checkpointMetaData.getCheckpointId(), exception);
		}
	}
}
