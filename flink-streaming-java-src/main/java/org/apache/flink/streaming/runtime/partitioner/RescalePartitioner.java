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

package org.apache.flink.streaming.runtime.partitioner;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * Partitioner that distributes the data equally by cycling through the output
 * channels. This distributes only to a subset of downstream nodes because
 * {@link org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator} instantiates
 * a {@link DistributionPattern#POINTWISE} distribution pattern when encountering
 * {@code RescalePartitioner}.
 * 
 * Partitioner 通过轮询将数据写入输出通道
 * 需要注意的是（这也是 RescalePartitioner 和 RebalancePartitioner 的区别）：
 * RescalePartitioner 仅仅将数据分配到下游节点的一个子集中，因为当 StreamingJobGraphGenerator
 * 遇到 RescalePartitioner 的时候，会实例化一个 POINTWISE 分配模式
 *
 * <p>The subset of downstream operations to which the upstream operation sends
 * elements depends on the degree of parallelism of both the upstream and downstream operation.
 * For example, if the upstream operation has parallelism 2 and the downstream operation
 * has parallelism 4, then one upstream operation would distribute elements to two
 * downstream operations while the other upstream operation would distribute to the other
 * two downstream operations. If, on the other hand, the downstream operation has parallelism
 * 2 while the upstream operation has parallelism 4 then two upstream operations will
 * distribute to one downstream operation while the other two upstream operations will
 * distribute to the other downstream operations.
 *
 * 上游操作 send 元素到下游操作的子集，这由上下游操作的并行度同时决定。举个例子，如果上游操作的并行度为 2
 * 下游操作的并行度为 4，一个上游操作将会分配元素到两个操作，另一个上游操作将分配元素到剩下的两个下游操作
 * 如果，从另一方面来说，上游操作的并行度为 4，下游操作的并行度为 2 的话，2 个上游操作会分配元素到一个下游
 * 操作，另外两个上游操作分配到剩下的一个下游操作中
 *
 * <p>In cases where the different parallelisms are not multiples of each other one or several
 * downstream operations will have a differing number of inputs from upstream operations.
 *
 * 如果上下游的并行度不是倍数的话，其中某些下游操作符将具有不同的输入数量
 * 
 * @param <T> Type of the elements in the Stream being rescaled
 */
@Internal
public class RescalePartitioner<T> extends StreamPartitioner<T> {
	private static final long serialVersionUID = 1L;

	private int nextChannelToSendTo = -1;

	@Override
	public int selectChannel(SerializationDelegate<StreamRecord<T>> record) {
		if (++nextChannelToSendTo >= numberOfChannels) {
			nextChannelToSendTo = 0;
		}
		return nextChannelToSendTo;
	}

	public StreamPartitioner<T> copy() {
		return this;
	}

	@Override
	public String toString() {
		return "RESCALE";
	}

