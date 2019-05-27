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

/**
 * An interface marking a task as capable of handling exceptions thrown
 * by different threads, other than the one executing the task itself.
 */
/**
 * 将任务标记为能够处理由不同线程抛出的异常的接口，而不是执行任务本身的线程
 */
public interface AsyncExceptionHandler {

	/**
	 * Handles an exception thrown by another thread (e.g. a TriggerTask),
	 * other than the one executing the main task.
	 */
	/**
	 * 处理另一个线程（例如TriggerTask）抛出的异常，而不是执行主任务的异常
	 */
	void handleAsyncException(String message, Throwable exception);
}
