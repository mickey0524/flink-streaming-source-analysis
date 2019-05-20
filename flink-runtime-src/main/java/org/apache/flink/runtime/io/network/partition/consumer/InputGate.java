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

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.runtime.event.TaskEvent;

import java.io.IOException;
import java.util.Optional;

/**
 * An input gate consumes one or more partitions of a single produced intermediate result.
 *
 * 一个 input gate 消耗单个产生的中间结果的一个或多个分区
 *
 * <p>Each intermediate result is partitioned over its producing parallel subtasks; each of these
 * partitions is furthermore partitioned into one or more subpartitions.
 * 
 * 每个中间结果都在其生成的并行子任务上进行分区
 * 此外，这些分区中的每一个都被划分为一个或多个子分区
 *
 * <p>As an example, consider a map-reduce program, where the map operator produces data and the
 * reduce operator consumes the produced data.
 * 
 * 例如，考虑一个 map-reduce 程序，map 操作符产生数据，reduce 操作符消费产生的数据
 *
 * <pre>{@code
 * +-----+              +---------------------+              +--------+
 * | Map | = produce => | Intermediate Result | <= consume = | Reduce |
 * +-----+              +---------------------+              +--------+
 * }</pre>
 *
 * <p>When deploying such a program in parallel, the intermediate result will be partitioned over its
 * producing parallel subtasks; each of these partitions is furthermore partitioned into one or more
 * subpartitions.
 *
 * 当并行部署这样的程序时，中间结果将在其生成的并行子任务上分区
 * 此外，这些分区中的每一个都被划分为一个或多个子分区
 * 
 * <pre>{@code
 *                            Intermediate result
 *               +-----------------------------------------+
 *               |                      +----------------+ |              +-----------------------+
 * +-------+     | +-------------+  +=> | Subpartition 1 | | <=======+=== | Input Gate | Reduce 1 |
 * | Map 1 | ==> | | Partition 1 | =|   +----------------+ |         |    +-----------------------+
 * +-------+     | +-------------+  +=> | Subpartition 2 | | <==+    |
 *               |                      +----------------+ |    |    | Subpartition request
 *               |                                         |    |    |
 *               |                      +----------------+ |    |    |
 * +-------+     | +-------------+  +=> | Subpartition 1 | | <==+====+
 * | Map 2 | ==> | | Partition 2 | =|   +----------------+ |    |         +-----------------------+
 * +-------+     | +-------------+  +=> | Subpartition 2 | | <==+======== | Input Gate | Reduce 2 |
 *               |                      +----------------+ |              +-----------------------+
 *               +-----------------------------------------+
 * }</pre>
 *
 * <p>In the above example, two map subtasks produce the intermediate result in parallel, resulting
 * in two partitions (Partition 1 and 2). Each of these partitions is further partitioned into two
 * subpartitions -- one for each parallel reduce subtask. As shown in the Figure, each reduce task
 * will have an input gate attached to it. This will provide its input, which will consist of one
 * subpartition from each partition of the intermediate result.
 * 
 * 在上面的例子中，两个 map 子任务并行产生中间结果，因此会出现两个分区（Partition 1 和 2）
 * 每个分区又被划分为两个子分区 - 每个子分区连接一个并行的 reduce 子任务
 * 和图中展示的一样，每个 reduce 任务会有一个 input gate 来连接它
 * 这将提供其输入，其将包含来自中间结果的每个分区的一个子分区
 */
public interface InputGate {

	int getNumberOfInputChannels();

	String getOwningTaskName();

	boolean isFinished();

	void requestPartitions() throws IOException, InterruptedException;

	/**
	 * Blocking call waiting for next {@link BufferOrEvent}.
	 *
	 * @return {@code Optional.empty()} if {@link #isFinished()} returns true.
	 */
	Optional<BufferOrEvent> getNextBufferOrEvent() throws IOException, InterruptedException;

	/**
	 * Poll the {@link BufferOrEvent}.
	 *
	 * @return {@code Optional.empty()} if there is no data to return or if {@link #isFinished()} returns true.
	 */
	Optional<BufferOrEvent> pollNextBufferOrEvent() throws IOException, InterruptedException;

	void sendTaskEvent(TaskEvent event) throws IOException;

	void registerListener(InputGateListener listener);

	int getPageSize();
}
