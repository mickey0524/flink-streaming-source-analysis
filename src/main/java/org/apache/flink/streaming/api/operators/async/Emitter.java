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

package org.apache.flink.streaming.api.operators.async;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.async.queue.AsyncCollectionResult;
import org.apache.flink.streaming.api.operators.async.queue.AsyncResult;
import org.apache.flink.streaming.api.operators.async.queue.AsyncWatermarkResult;
import org.apache.flink.streaming.api.operators.async.queue.StreamElementQueue;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * Runnable responsible for consuming elements from the given queue and outputting them to the
 * given output/timestampedCollector.
 *
 * @param <OUT> Type of the output elements
 */
/**
 * Runnable 负责消耗给定队列中的元素并将它们输出到给定的 output/timestampedCollector
 */
@Internal
public class Emitter<OUT> implements Runnable {

	private static final Logger LOG = LoggerFactory.getLogger(Emitter.class);

	/** Lock to hold before outputting. */
	// 在 output 之前需要加速
	private final Object checkpointLock;

	/** Output for the watermark elements. */
	// 输出 watermark
	private final Output<StreamRecord<OUT>> output;

	/** Queue to consume the async results from. */
	// 消费队列中的异步结果
	private final StreamElementQueue streamElementQueue;

	private final OperatorActions operatorActions;

	/** Output for stream records. */
	// 输出 StreamRecord
	private final TimestampedCollector<OUT> timestampedCollector;

	// Emitter 是否处于运行状态
	private volatile boolean running;

	public Emitter(
			final Object checkpointLock,
			final Output<StreamRecord<OUT>> output,
			final StreamElementQueue streamElementQueue,
			final OperatorActions operatorActions) {

		this.checkpointLock = Preconditions.checkNotNull(checkpointLock, "checkpointLock");
		this.output = Preconditions.checkNotNull(output, "output");
		this.streamElementQueue = Preconditions.checkNotNull(streamElementQueue, "streamElementQueue");
		this.operatorActions = Preconditions.checkNotNull(operatorActions, "operatorActions");

		this.timestampedCollector = new TimestampedCollector<>(this.output);
		this.running = true;
	}

	@Override
	public void run() {
		try {
			while (running) {
				LOG.debug("Wait for next completed async stream element result.");
				// 阻塞等待下一个队列中完成异步操作的元素
				AsyncResult streamElementEntry = streamElementQueue.peekBlockingly();

				output(streamElementEntry);
			}
		} catch (InterruptedException e) {
			if (running) {
				operatorActions.failOperator(e);
			} else {
				// Thread got interrupted which means that it should shut down
				LOG.debug("Emitter thread got interrupted, shutting down.");
			}
		} catch (Throwable t) {
			operatorActions.failOperator(new Exception("AsyncWaitOperator's emitter caught an " +
				"unexpected throwable.", t));
		}
	}

	// 输出已完成的异步操作
	private void output(AsyncResult asyncResult) throws InterruptedException {
		// 如果是 watermark 的话
		if (asyncResult.isWatermark()) {
			synchronized (checkpointLock) {
				// 将 asyncResult 转为 watermark
				AsyncWatermarkResult asyncWatermarkResult = asyncResult.asWatermark();

				LOG.debug("Output async watermark.");
				// 输出 watermark
				output.emitWatermark(asyncWatermarkResult.getWatermark());

				// remove the peeked element from the async collector buffer so that it is no longer
				// checkpointed
				// 从异步收集器缓冲区中删除 peeked 元素，以便不再检查点
				streamElementQueue.poll();

				// notify the main thread that there is again space left in the async collector
				// buffer
				// 通知主线程异步收集器缓冲区中还剩余空间
				checkpointLock.notifyAll();
			}
		} else {
			// 将 asyncResult 转为结果集合
			AsyncCollectionResult<OUT> streamRecordResult = asyncResult.asResultCollection();

			if (streamRecordResult.hasTimestamp()) {
				timestampedCollector.setAbsoluteTimestamp(streamRecordResult.getTimestamp());
			} else {
				timestampedCollector.eraseTimestamp();
			}

			synchronized (checkpointLock) {
				LOG.debug("Output async stream element collection result.");

				try {
					Collection<OUT> resultCollection = streamRecordResult.get();
					// 结果集合中的 StreamRecord 时间戳相同
					if (resultCollection != null) {
						for (OUT result : resultCollection) {
							timestampedCollector.collect(result);
						}
					}
				} catch (Exception e) {
					operatorActions.failOperator(
						new Exception("An async function call terminated with an exception. " +
							"Failing the AsyncWaitOperator.", e));
				}

				// remove the peeked element from the async collector buffer so that it is no longer
				// checkpointed
				streamElementQueue.poll();

				// notify the main thread that there is again space left in the async collector
				// buffer
				checkpointLock.notifyAll();
			}
		}
	}

	// stop 方法停止 Emitter，将 running 变量设置为 false
	public void stop() {
		running = false;
	}
}
