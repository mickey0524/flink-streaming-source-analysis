/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.BlockingQueueBroker;
import org.apache.flink.streaming.runtime.io.RecordWriterOutput;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * A special {@link StreamTask} that is used for executing feedback edges. This is used in
 * combination with {@link StreamIterationTail}.
 */
/**
 * 一个特殊的StreamTask，用于执行反馈边
 * 它与StreamIterationTail结合使用
 */
@Internal
public class StreamIterationHead<OUT> extends OneInputStreamTask<OUT, OUT> {

	private static final Logger LOG = LoggerFactory.getLogger(StreamIterationHead.class);

	private volatile boolean running = true;

	public StreamIterationHead(Environment env) {
		super(env);
	}

	// ------------------------------------------------------------------------

	@Override
	protected void run() throws Exception {

		final String iterationId = getConfiguration().getIterationId();  // broker-${StreamTransformation.id}
		if (iterationId == null || iterationId.length() == 0) {
			throw new Exception("Missing iteration ID in the task configuration");
		}

		final String brokerID = createBrokerIdString(getEnvironment().getJobID(), iterationId ,
				getEnvironment().getTaskInfo().getIndexOfThisSubtask());  // 生成 brokerID

		final long iterationWaitTime = getConfiguration().getIterationWaitTime();  // 获取反馈 waitTime
		final boolean shouldWait = iterationWaitTime > 0;  // 大于 0 表示显式设置了等待时间，waitTime 之内没有数据反馈，停止反馈

		// 创建一个 BlockingQueue
		final BlockingQueue<StreamRecord<OUT>> dataChannel = new ArrayBlockingQueue<StreamRecord<OUT>>(1);

		// offer the queue for the tail
		BlockingQueueBroker.INSTANCE.handIn(brokerID, dataChannel);
		LOG.info("Iteration head {} added feedback queue under {}", getName(), brokerID);

		// do the work
		try {
			@SuppressWarnings("unchecked")
			RecordWriterOutput<OUT>[] outputs = (RecordWriterOutput<OUT>[]) getStreamOutputs();  // 获取流输出

			// If timestamps are enabled we make sure to remove cyclic watermark dependencies
			// 如果启用了时间戳，我们确保删除循环水印依赖
			if (isSerializingTimestamps()) {
				synchronized (getCheckpointLock()) {
					for (RecordWriterOutput<OUT> output : outputs) {
						output.emitWatermark(new Watermark(Long.MAX_VALUE));
					}
				}
			}

			while (running) {
				// 如果设置了等待时机，则 BlockingQueue 最多等待 iterationWaitTime
				StreamRecord<OUT> nextRecord = shouldWait ?
					dataChannel.poll(iterationWaitTime, TimeUnit.MILLISECONDS) :
					dataChannel.take();

				if (nextRecord != null) {
					synchronized (getCheckpointLock()) {
						for (RecordWriterOutput<OUT> output : outputs) {
							output.collect(nextRecord);
						}
					}
				}
				else {
					// done
					break;
				}
			}
		}
		finally {
			// 等待时间过了，没有数据
			// make sure that we remove the queue from the broker, to prevent a resource leak
			// 确保我们从代理中删除队列，以防止资源泄漏
			BlockingQueueBroker.INSTANCE.remove(brokerID);
			LOG.info("Iteration head {} removed feedback queue under {}", getName(), brokerID);
		}
	}

	@Override
	protected void cancelTask() {
		running = false;
	}

	// ------------------------------------------------------------------------

	@Override
	public void init() {
		// does not hold any resources, no initialization necessary
	}

	@Override
	protected void cleanup() throws Exception {
		// does not hold any resources, no cleanup necessary
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	/**
	 * Creates the identification string with which head and tail task find the shared blocking
	 * queue for the back channel. The identification string is unique per parallel head/tail pair
	 * per iteration per job.
	 *
	 * @param jid The job ID.
	 * @param iterationID The id of the iteration in the job.
	 * @param subtaskIndex The parallel subtask number
	 * @return The identification string.
	 */
	/**
	 * 创建标识字符串，head 和 tail 任务使用该标识字符串查找反向通道的共享阻塞队列
	 * 每个作业每次迭代，每个并行头/尾对的标识字符串是唯一的
	 */
	public static String createBrokerIdString(JobID jid, String iterationID, int subtaskIndex) {
		return jid + "-" + iterationID + "-" + subtaskIndex;
	}
}
