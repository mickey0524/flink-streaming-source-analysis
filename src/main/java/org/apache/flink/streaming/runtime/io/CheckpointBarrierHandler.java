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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;

import java.io.IOException;

/**
 * The CheckpointBarrierHandler reacts to checkpoint barrier arriving from the input channels.
 * Different implementations may either simply track barriers, or block certain inputs on
 * barriers.
 */
/**
 * CheckpointBarrierHandler 对来自输入通道的检查点障碍作出反应
 * 不同的实现可以简单地跟踪障碍，或阻止障碍上的某些输入
 */
@Internal
public interface CheckpointBarrierHandler {

	/**
	 * Returns the next {@link BufferOrEvent} that the operator may consume.
	 * This call blocks until the next BufferOrEvent is available, or until the stream
	 * has been determined to be finished.
	 *
	 * @return The next BufferOrEvent, or {@code null}, if the stream is finished.
	 *
	 * @throws IOException Thrown if the network or local disk I/O fails.
	 *
	 * @throws InterruptedException Thrown if the thread is interrupted while blocking during
	 *                              waiting for the next BufferOrEvent to become available.
	 * @throws Exception Thrown in case that a checkpoint fails that is started as the result of receiving
	 *                   the last checkpoint barrier
	 */
	/**
	 * 返回运算符可能使用的下一个 BufferOrEvent
	 * 此调用将阻塞，直到下一个 BufferOrEvent 可用，或者直到确定流已完成为止
	 */
	BufferOrEvent getNextNonBlocked() throws Exception;

	/**
	 * Registers the task be notified once all checkpoint barriers have been received for a checkpoint.
	 *
	 * @param task The task to notify
	 */
	/**
	 * 一旦收到检查点的所有检查点障碍，就会通知注册任务
	 */
	void registerCheckpointEventHandler(AbstractInvokable task);

	/**
	 * Cleans up all internally held resources.
	 *
	 * @throws IOException Thrown if the cleanup of I/O resources failed.
	 */
	/**
	 * 清理所有内部资源
	 */
	void cleanup() throws IOException;

	/**
	 * Checks if the barrier handler has buffered any data internally.
	 * @return {@code True}, if no data is buffered internally, {@code false} otherwise.
	 */
	/**
	 * 检查屏障处理程序是否在内部缓冲了任何数据
	 */
	boolean isEmpty();

	/**
	 * Gets the time that the latest alignment took, in nanoseconds.
	 * If there is currently an alignment in progress, it will return the time spent in the
	 * current alignment so far.
	 *
	 * @return The duration in nanoseconds
	 */
	/**
	 * 获取最新对齐所用的时间，以纳秒为单位
	 * 如果当前正在进行对齐，则它将返回到目前为止在当前对齐中花费的时间
	 */
	long getAlignmentDurationNanos();
}
