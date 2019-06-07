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

package org.apache.flink.streaming.api.graph;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.InputTypeConfigurable;
import org.apache.flink.api.java.typeutils.MissingTypeInfo;
import org.apache.flink.optimizer.plan.StreamingPlan;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.OutputTypeConfigurable;
import org.apache.flink.streaming.api.operators.StoppableStreamSource;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.partitioner.RebalancePartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.SourceStreamTask;
import org.apache.flink.streaming.runtime.tasks.StoppableSourceStreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamIterationHead;
import org.apache.flink.streaming.runtime.tasks.StreamIterationTail;
import org.apache.flink.streaming.runtime.tasks.TwoInputStreamTask;
import org.apache.flink.util.OutputTag;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Class representing the streaming topology. It contains all the information
 * necessary to build the jobgraph for the execution.
 *
 */
/**
 * 代表流拓扑的类，它包含了构建执行所用的 jobgraph 需要的所有信息
 * 最后 StreamGraph 中的节点都是包含 StreamOperator 的 transformation
 * select，sideOutput，partition 都会成为 StreamEdge 的属性
 */
@Internal
public class StreamGraph extends StreamingPlan {

	private static final Logger LOG = LoggerFactory.getLogger(StreamGraph.class);

	private String jobName = StreamExecutionEnvironment.DEFAULT_JOB_NAME;  // Flink Streaming Job

	private final StreamExecutionEnvironment environment;  // 执行环境
	private final ExecutionConfig executionConfig;  // 执行配置
	private final CheckpointConfig checkpointConfig;  // 检查点配置

	private boolean chaining;

	private Map<Integer, StreamNode> streamNodes;  // 节点 map，key 是 transformation 的 id
	private Set<Integer> sources;  // 图中所有的数据源头节点
	private Set<Integer> sinks;  // 图中所有的下沉节点
	private Map<Integer, Tuple2<Integer, List<String>>> virtualSelectNodes;  // 图中所有的 select 虚拟节点
	private Map<Integer, Tuple2<Integer, OutputTag>> virtualSideOutputNodes;  // 图中所有的 side output 虚拟节点
	private Map<Integer, Tuple2<Integer, StreamPartitioner<?>>> virtualPartitionNodes;  // 图中所有的 partition 虚拟节点

	protected Map<Integer, String> vertexIDtoBrokerID;  // 存储反馈头尾节点 id 和 FeedbackTransformation id 之间的映射
	protected Map<Integer, Long> vertexIDtoLoopTimeout;  // 存储反馈头尾节点 id 和 iterate 中设置的 timeout 之间的映射
	private StateBackend stateBackend;
	private Set<Tuple2<StreamNode, StreamNode>> iterationSourceSinkPairs;  // 存储图中所有迭代头、尾组成的 pair

	public StreamGraph(StreamExecutionEnvironment environment) {
		this.environment = environment;
		this.executionConfig = environment.getConfig();
		this.checkpointConfig = environment.getCheckpointConfig();

		// create an empty new stream graph.
		// 创建一个空的新流图
		clear();
	}

	/**
	 * Remove all registered nodes etc.
	 */
	public void clear() {
		streamNodes = new HashMap<>();
		virtualSelectNodes = new HashMap<>();
		virtualSideOutputNodes = new HashMap<>();
		virtualPartitionNodes = new HashMap<>();
		vertexIDtoBrokerID = new HashMap<>();
		vertexIDtoLoopTimeout  = new HashMap<>();
		iterationSourceSinkPairs = new HashSet<>();
		sources = new HashSet<>();
		sinks = new HashSet<>();
	}

	public StreamExecutionEnvironment getEnvironment() {
		return environment;
	}

	public ExecutionConfig getExecutionConfig() {
		return executionConfig;
	}

	public CheckpointConfig getCheckpointConfig() {
		return checkpointConfig;
	}

	public String getJobName() {
		return jobName;
	}

	public void setJobName(String jobName) {
		this.jobName = jobName;
	}

	public void setChaining(boolean chaining) {
		this.chaining = chaining;
	}

	public void setStateBackend(StateBackend backend) {
		this.stateBackend = backend;
	}

	public StateBackend getStateBackend() {
		return this.stateBackend;
	}

	// Checkpointing

	public boolean isChainingEnabled() {
		return chaining;
	}
	
	// 根据 vertexIDtoLoopTimeout 是否为空来得出图是否是迭代的
	public boolean isIterative() {
		return !vertexIDtoLoopTimeout.isEmpty();
	}

	/**
	 * 添加源 transformation
	 */
	public <IN, OUT> void addSource(Integer vertexID,
		String slotSharingGroup,
		@Nullable String coLocationGroup,
		StreamOperator<OUT> operatorObject,
		TypeInformation<IN> inTypeInfo,
		TypeInformation<OUT> outTypeInfo,
		String operatorName) {
		addOperator(vertexID, slotSharingGroup, coLocationGroup, operatorObject, inTypeInfo, outTypeInfo, operatorName);
		sources.add(vertexID);
	}

	/**
	 * 添加 sink transformation
	 */
	public <IN, OUT> void addSink(Integer vertexID,
		String slotSharingGroup,
		@Nullable String coLocationGroup,
		StreamOperator<OUT> operatorObject,
		TypeInformation<IN> inTypeInfo,
		TypeInformation<OUT> outTypeInfo,
		String operatorName) {
		addOperator(vertexID, slotSharingGroup, coLocationGroup, operatorObject, inTypeInfo, outTypeInfo, operatorName);
		sinks.add(vertexID);
	}

	/**
	 * 添加操作符
	 */
	public <IN, OUT> void addOperator(
			Integer vertexID,
			String slotSharingGroup,
			@Nullable String coLocationGroup,
			StreamOperator<OUT> operatorObject,
			TypeInformation<IN> inTypeInfo,
			TypeInformation<OUT> outTypeInfo,
			String operatorName) {

		if (operatorObject instanceof StoppableStreamSource) {
			addNode(vertexID, slotSharingGroup, coLocationGroup, StoppableSourceStreamTask.class, operatorObject, operatorName);
		} else if (operatorObject instanceof StreamSource) {
			addNode(vertexID, slotSharingGroup, coLocationGroup, SourceStreamTask.class, operatorObject, operatorName);
		} else {
			addNode(vertexID, slotSharingGroup, coLocationGroup, OneInputStreamTask.class, operatorObject, operatorName);
		}

		TypeSerializer<IN> inSerializer = inTypeInfo != null && !(inTypeInfo instanceof MissingTypeInfo) ? inTypeInfo.createSerializer(executionConfig) : null;

		TypeSerializer<OUT> outSerializer = outTypeInfo != null && !(outTypeInfo instanceof MissingTypeInfo) ? outTypeInfo.createSerializer(executionConfig) : null;

		// 根据输入输出类型设置类型序列器
		setSerializers(vertexID, inSerializer, null, outSerializer);

		if (operatorObject instanceof OutputTypeConfigurable && outTypeInfo != null) {
			@SuppressWarnings("unchecked")
			OutputTypeConfigurable<OUT> outputTypeConfigurable = (OutputTypeConfigurable<OUT>) operatorObject;
			// sets the output type which must be know at StreamGraph creation time
			outputTypeConfigurable.setOutputType(outTypeInfo, executionConfig);
		}

		if (operatorObject instanceof InputTypeConfigurable) {
			InputTypeConfigurable inputTypeConfigurable = (InputTypeConfigurable) operatorObject;
			inputTypeConfigurable.setInputType(inTypeInfo, executionConfig);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("Vertex: {}", vertexID);
		}
	}

	/**
	 * 添加连通操作符，有两个输入
	 */
	public <IN1, IN2, OUT> void addCoOperator(
			Integer vertexID,
			String slotSharingGroup,
			@Nullable String coLocationGroup,
			TwoInputStreamOperator<IN1, IN2, OUT> taskOperatorObject,
			TypeInformation<IN1> in1TypeInfo,
			TypeInformation<IN2> in2TypeInfo,
			TypeInformation<OUT> outTypeInfo,
			String operatorName) {

		addNode(vertexID, slotSharingGroup, coLocationGroup, TwoInputStreamTask.class, taskOperatorObject, operatorName);

		TypeSerializer<OUT> outSerializer = (outTypeInfo != null) && !(outTypeInfo instanceof MissingTypeInfo) ?
				outTypeInfo.createSerializer(executionConfig) : null;

		setSerializers(vertexID, in1TypeInfo.createSerializer(executionConfig), in2TypeInfo.createSerializer(executionConfig), outSerializer);

		if (taskOperatorObject instanceof OutputTypeConfigurable) {
			@SuppressWarnings("unchecked")
			OutputTypeConfigurable<OUT> outputTypeConfigurable = (OutputTypeConfigurable<OUT>) taskOperatorObject;
			// sets the output type which must be know at StreamGraph creation time
			outputTypeConfigurable.setOutputType(outTypeInfo, executionConfig);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("CO-TASK: {}", vertexID);
		}
	}

	/**
	 * 添加节点
	 */
	protected StreamNode addNode(Integer vertexID,
		String slotSharingGroup,
		@Nullable String coLocationGroup,
		Class<? extends AbstractInvokable> vertexClass,
		StreamOperator<?> operatorObject,
		String operatorName) {

		/**
		 * 如果图里已经存在 vertexID 这个节点，抛出重复节点的异常
		 */
		if (streamNodes.containsKey(vertexID)) {
			throw new RuntimeException("Duplicate vertexID " + vertexID);
		}

		StreamNode vertex = new StreamNode(environment,
			vertexID,
			slotSharingGroup,
			coLocationGroup,
			operatorObject,
			operatorName,
			new ArrayList<OutputSelector<?>>(),
			vertexClass);

		/**
		 * 将 (id, 节点) 加入哈希表，避免重复添加节点
		 */
		streamNodes.put(vertexID, vertex);

		return vertex;
	}

	/**
	 * Adds a new virtual node that is used to connect a downstream vertex to only the outputs
	 * with the selected names.
	 *
	 * 添加用于将下游顶点仅连接到具有选定名称的输出的新虚拟节点
	 *
	 * <p>When adding an edge from the virtual node to a downstream node the connection will be made
	 * to the original node, only with the selected names given here.
	 *
	 * 当在虚拟节点和下游节点间添加一条边的时候，将只使用此处给定的 selected names 连接到原始节点
	 *
	 * @param originalId ID of the node that should be connected to.
	 * @param virtualId ID of the virtual node.
	 * @param selectedNames The selected names.
	 */
	public void addVirtualSelectNode(Integer originalId, Integer virtualId, List<String> selectedNames) {

		if (virtualSelectNodes.containsKey(virtualId)) {
			throw new IllegalStateException("Already has virtual select node with id " + virtualId);
		}

		virtualSelectNodes.put(virtualId,
				new Tuple2<Integer, List<String>>(originalId, selectedNames));
	}

	/**
	 * Adds a new virtual node that is used to connect a downstream vertex to only the outputs with
	 * the selected side-output {@link OutputTag}.
	 *
	 * @param originalId ID of the node that should be connected to.
	 * @param virtualId ID of the virtual node.
	 * @param outputTag The selected side-output {@code OutputTag}.
	 */
	/**
	 * 添加用于将下游顶点仅连接到具有选定 side-output 的输出的新虚拟节点
	 */
	public void addVirtualSideOutputNode(Integer originalId, Integer virtualId, OutputTag outputTag) {

		if (virtualSideOutputNodes.containsKey(virtualId)) {
			throw new IllegalStateException("Already has virtual output node with id " + virtualId);
		}

		// verify that we don't already have a virtual node for the given originalId/outputTag
		// combination with a different TypeInformation. This would indicate that someone is trying
		// to read a side output from an operation with a different type for the same side output
		// id.

		// 验证我们之前没有添加过和 originalId/outputTag 相同，TypeInformation 不同的虚拟节点
		// 这表示有人试图从具有不同类型的操作中读取同一个侧输出 ID 的侧输出

		for (Tuple2<Integer, OutputTag> tag : virtualSideOutputNodes.values()) {
			if (!tag.f0.equals(originalId)) {
				// different source operator
				continue;
			}

			if (tag.f1.getId().equals(outputTag.getId()) &&
					!tag.f1.getTypeInfo().equals(outputTag.getTypeInfo())) {
				throw new IllegalArgumentException("Trying to add a side output for the same " +
						"side-output id with a different type. This is not allowed. Side-output ID: " +
						tag.f1.getId());
			}
		}

		virtualSideOutputNodes.put(virtualId, new Tuple2<>(originalId, outputTag));
	}

	/**
	 * Adds a new virtual node that is used to connect a downstream vertex to an input with a
	 * certain partitioning.
	 *
	 * 添加用于将下游顶点仅连接到具有确定分区的输出的新虚拟节点
	 *
	 * <p>When adding an edge from the virtual node to a downstream node the connection will be made
	 * to the original node, but with the partitioning given here.
	 *
	 * 当在虚拟节点和下游节点间添加一条边的时候，将只使用此处给定的 partitioner 连接到原始节点
	 *
	 * @param originalId ID of the node that should be connected to.
	 * @param virtualId ID of the virtual node.
	 * @param partitioner The partitioner
	 */
	public void addVirtualPartitionNode(Integer originalId, Integer virtualId, StreamPartitioner<?> partitioner) {

		if (virtualPartitionNodes.containsKey(virtualId)) {
			throw new IllegalStateException("Already has virtual partition node with id " + virtualId);
		}

		virtualPartitionNodes.put(virtualId,
				new Tuple2<Integer, StreamPartitioner<?>>(originalId, partitioner));
	}

	/**
	 * Determines the slot sharing group of an operation across virtual nodes.
	 */
	/**
	 * 确定跨虚拟节点的操作的槽共享组
	 */
	public String getSlotSharingGroup(Integer id) {
		// 虚拟节点都由 originalId 来决定
		if (virtualSideOutputNodes.containsKey(id)) {
			Integer mappedId = virtualSideOutputNodes.get(id).f0;
			return getSlotSharingGroup(mappedId);
		} else if (virtualSelectNodes.containsKey(id)) {
			Integer mappedId = virtualSelectNodes.get(id).f0;
			return getSlotSharingGroup(mappedId);
		} else if (virtualPartitionNodes.containsKey(id)) {
			Integer mappedId = virtualPartitionNodes.get(id).f0;
			return getSlotSharingGroup(mappedId);
		} else {
			StreamNode node = getStreamNode(id);
			return node.getSlotSharingGroup();
		}
	}

	/**
	 * 为 StreamGraph 中的两个节点连上边（暴露给外部使用的方法）
	 * @param upStreamVertexID 边的 source 节点
	 * @param downStreamVertexID 边的 target 节点
	 * @param typeNumber 边的类型
	 */
	public void addEdge(Integer upStreamVertexID, Integer downStreamVertexID, int typeNumber) {
		addEdgeInternal(upStreamVertexID,
				downStreamVertexID,
				typeNumber,
				null,
				new ArrayList<String>(),
				null);

	}

	/**
	 * 为 StreamGraph 中的两个节点连上边（内部使用的方法），会被递归调用
	 * SideOutput、Select、Partition 不会在 StreamGraph 中存在真正的节点
	 * 它们的选择器会作为属性写入 StreamEdge 中
	 */
	private void addEdgeInternal(Integer upStreamVertexID,
			Integer downStreamVertexID,
			int typeNumber,
			StreamPartitioner<?> partitioner,
			List<String> outputNames,
			OutputTag outputTag) {
		
		// 上游节点是 SideOutputNode 的时候
		if (virtualSideOutputNodes.containsKey(upStreamVertexID)) {
			int virtualId = upStreamVertexID;
			upStreamVertexID = virtualSideOutputNodes.get(virtualId).f0;
			if (outputTag == null) {
				outputTag = virtualSideOutputNodes.get(virtualId).f1;
			}
			addEdgeInternal(upStreamVertexID, downStreamVertexID, typeNumber, partitioner, null, outputTag);
		} else if (virtualSelectNodes.containsKey(upStreamVertexID)) {
			// 上游节点是 SelectNode 的时候
			int virtualId = upStreamVertexID;
			upStreamVertexID = virtualSelectNodes.get(virtualId).f0;
			if (outputNames.isEmpty()) {
				// selections that happen downstream override earlier selections
				outputNames = virtualSelectNodes.get(virtualId).f1;
			}
			addEdgeInternal(upStreamVertexID, downStreamVertexID, typeNumber, partitioner, outputNames, outputTag);
		} else if (virtualPartitionNodes.containsKey(upStreamVertexID)) {
			// 上游节点是 Partitioner 节点的时候
			int virtualId = upStreamVertexID;
			upStreamVertexID = virtualPartitionNodes.get(virtualId).f0;
			if (partitioner == null) {
				partitioner = virtualPartitionNodes.get(virtualId).f1;
			}
			addEdgeInternal(upStreamVertexID, downStreamVertexID, typeNumber, partitioner, outputNames, outputTag);
		} else {
			StreamNode upstreamNode = getStreamNode(upStreamVertexID);
			StreamNode downstreamNode = getStreamNode(downStreamVertexID);

			// If no partitioner was specified and the parallelism of upstream and downstream
			// operator matches use forward partitioning, use rebalance otherwise.
			// 如果没有显示定义 partitioner，同时上下游操作符满足使用 forward partitioning 的条件，使用
			// forward partitioning，否则使用 rebalance
			if (partitioner == null && upstreamNode.getParallelism() == downstreamNode.getParallelism()) {
				partitioner = new ForwardPartitioner<Object>();
			} else if (partitioner == null) {
				partitioner = new RebalancePartitioner<Object>();
			}

			if (partitioner instanceof ForwardPartitioner) {
				if (upstreamNode.getParallelism() != downstreamNode.getParallelism()) {
					throw new UnsupportedOperationException("Forward partitioning does not allow " +
							"change of parallelism. Upstream operation: " + upstreamNode + " parallelism: " + upstreamNode.getParallelism() +
							", downstream operation: " + downstreamNode + " parallelism: " + downstreamNode.getParallelism() +
							" You must use another partitioning strategy, such as broadcast, rebalance, shuffle or global.");
				}
			}

			StreamEdge edge = new StreamEdge(upstreamNode, downstreamNode, typeNumber, outputNames, partitioner, outputTag);

			// 将边加入两端节点的入边集合和出边集合
			getStreamNode(edge.getSourceId()).addOutEdge(edge);
			getStreamNode(edge.getTargetId()).addInEdge(edge);
		}
	}

	/**
	 * 为 StreamNode 添加 outputSelector
	 * 虚拟节点的话，加在 originalId 节点上
	 * 这里不用考虑 SideOutputNode，因为
	 * StreamGraphGenerator 中有对 Split 的检测，Split 和 SideOutput 不能被同时使用
	 */
	public <T> void addOutputSelector(Integer vertexID, OutputSelector<T> outputSelector) {
		if (virtualPartitionNodes.containsKey(vertexID)) {
			addOutputSelector(virtualPartitionNodes.get(vertexID).f0, outputSelector);
		} else if (virtualSelectNodes.containsKey(vertexID)) {
			addOutputSelector(virtualSelectNodes.get(vertexID).f0, outputSelector);
		} else {
			getStreamNode(vertexID).addOutputSelector(outputSelector);

			if (LOG.isDebugEnabled()) {
				LOG.debug("Outputselector set for {}", vertexID);
			}
		}

	}
	
	// 为 StreamNode 设置并行度
	public void setParallelism(Integer vertexID, int parallelism) {
		if (getStreamNode(vertexID) != null) {
			getStreamNode(vertexID).setParallelism(parallelism);
		}
	}

	// 为 StreamNode 设置最大并行度
	public void setMaxParallelism(int vertexID, int maxParallelism) {
		if (getStreamNode(vertexID) != null) {
			getStreamNode(vertexID).setMaxParallelism(maxParallelism);
		}
	}

	// 为 StreamNode 设置最大最小资源
	public void setResources(int vertexID, ResourceSpec minResources, ResourceSpec preferredResources) {
		if (getStreamNode(vertexID) != null) {
			getStreamNode(vertexID).setResources(minResources, preferredResources);
		}
	}

	// 为 Keyed Stream 设置 keySelector
	public void setOneInputStateKey(Integer vertexID, KeySelector<?, ?> keySelector, TypeSerializer<?> keySerializer) {
		StreamNode node = getStreamNode(vertexID);
		node.setStatePartitioner1(keySelector);
		node.setStateKeySerializer(keySerializer);
	}

	public void setTwoInputStateKey(Integer vertexID, KeySelector<?, ?> keySelector1, KeySelector<?, ?> keySelector2, TypeSerializer<?> keySerializer) {
		StreamNode node = getStreamNode(vertexID);
		node.setStatePartitioner1(keySelector1);
		node.setStatePartitioner2(keySelector2);
		node.setStateKeySerializer(keySerializer);
	}

	// 为 StreamNode 设置 bufferTimeout
	public void setBufferTimeout(Integer vertexID, long bufferTimeout) {
		if (getStreamNode(vertexID) != null) {
			getStreamNode(vertexID).setBufferTimeout(bufferTimeout);
		}
	}

	/**
	 * 设置序列化
	 */
	public void setSerializers(Integer vertexID, TypeSerializer<?> in1, TypeSerializer<?> in2, TypeSerializer<?> out) {
		StreamNode vertex = getStreamNode(vertexID);
		vertex.setSerializerIn1(in1);
		vertex.setSerializerIn2(in2);
		vertex.setSerializerOut(out);
	}

	public void setSerializersFrom(Integer from, Integer to) {
		StreamNode fromVertex = getStreamNode(from);
		StreamNode toVertex = getStreamNode(to);

		toVertex.setSerializerIn1(fromVertex.getTypeSerializerOut());
		toVertex.setSerializerOut(fromVertex.getTypeSerializerIn1());
	}

	public <OUT> void setOutType(Integer vertexID, TypeInformation<OUT> outType) {
		getStreamNode(vertexID).setSerializerOut(outType.createSerializer(executionConfig));
	}

	public <IN, OUT> void setOperator(Integer vertexID, StreamOperator<OUT> operatorObject) {
		getStreamNode(vertexID).setOperator(operatorObject);
	}

	public void setInputFormat(Integer vertexID, InputFormat<?, ?> inputFormat) {
		getStreamNode(vertexID).setInputFormat(inputFormat);
	}

	// 为 StreamNode 设置 uid
	void setTransformationUID(Integer nodeId, String transformationId) {
		StreamNode node = streamNodes.get(nodeId);
		if (node != null) {
			node.setTransformationUID(transformationId);
		}
	}

	// 为 StreamNode 设置 user hash code
	void setTransformationUserHash(Integer nodeId, String nodeHash) {
		StreamNode node = streamNodes.get(nodeId);
		if (node != null) {
			node.setUserHash(nodeHash);

		}
	}

	/**
	 * 通过节点 id 获取 StreamNode
	 */
	public StreamNode getStreamNode(Integer vertexID) {
		return streamNodes.get(vertexID);
	}

	// 获取节点 ID 集合
	protected Collection<? extends Integer> getVertexIDs() {
		return streamNodes.keySet();
	}

	// 获取 sourceId 和 targetId 之间的边集合
	public List<StreamEdge> getStreamEdges(int sourceId, int targetId) {

		List<StreamEdge> result = new ArrayList<>();
		for (StreamEdge edge : getStreamNode(sourceId).getOutEdges()) {
			if (edge.getTargetId() == targetId) {
				result.add(edge);
			}
		}

		if (result.isEmpty()) {
			throw new RuntimeException("No such edge in stream graph: " + sourceId + " -> " + targetId);
		}

		return result;
	}

	// 获取所有的源 ID
	public Collection<Integer> getSourceIDs() {
		return sources;
	}

	// 获取所有的 sink ID
	public Collection<Integer> getSinkIDs() {
		return sinks;
	}

	// 获取所有的 StreamNode
	public Collection<StreamNode> getStreamNodes() {
		return streamNodes.values();
	}

	public Set<Tuple2<Integer, StreamOperator<?>>> getOperators() {
		Set<Tuple2<Integer, StreamOperator<?>>> operatorSet = new HashSet<>();
		for (StreamNode vertex : streamNodes.values()) {
			operatorSet.add(new Tuple2<Integer, StreamOperator<?>>(vertex.getId(), vertex
					.getOperator()));
		}
		return operatorSet;
	}

	public String getBrokerID(Integer vertexID) {
		return vertexIDtoBrokerID.get(vertexID);
	}

	public long getLoopTimeout(Integer vertexID) {
		return vertexIDtoLoopTimeout.get(vertexID);
	}
	
	/**
	 * 创建迭代的 source 和 sink
	 */
	public Tuple2<StreamNode, StreamNode> createIterationSourceAndSink(
		int loopId,
		int sourceId,
		int sinkId,
		long timeout,
		int parallelism,
		int maxParallelism,
		ResourceSpec minResources,
		ResourceSpec preferredResources) {
		// 创建迭代源头节点
		StreamNode source = this.addNode(sourceId,
			null,
			null,
			StreamIterationHead.class,  // task 类
			null,
			"IterationSource-" + loopId);
		sources.add(source.getId());
		setParallelism(source.getId(), parallelism);
		setMaxParallelism(source.getId(), maxParallelism);
		setResources(source.getId(), minResources, preferredResources);

		StreamNode sink = this.addNode(sinkId,
			null,
			null,
			StreamIterationTail.class,  // task 类
			null,
			"IterationSink-" + loopId);
		sinks.add(sink.getId());
		setParallelism(sink.getId(), parallelism);
		setMaxParallelism(sink.getId(), parallelism);

		iterationSourceSinkPairs.add(new Tuple2<>(source, sink));

		this.vertexIDtoBrokerID.put(source.getId(), "broker-" + loopId);
		this.vertexIDtoBrokerID.put(sink.getId(), "broker-" + loopId);
		this.vertexIDtoLoopTimeout.put(source.getId(), timeout);
		this.vertexIDtoLoopTimeout.put(sink.getId(), timeout);

		return new Tuple2<>(source, sink);
	}

	// 返回所有的 iterationSourceSinkPairs
	public Set<Tuple2<StreamNode, StreamNode>> getIterationSourceSinkPairs() {
		return iterationSourceSinkPairs;
	}
	
	// 获取 edge 的源头 StreamNode
	public StreamNode getSourceVertex(StreamEdge edge) {
		return streamNodes.get(edge.getSourceId());
	}

	// 获取 edge 的目标 StreamNode
	public StreamNode getTargetVertex(StreamEdge edge) {
		return streamNodes.get(edge.getTargetId());
	}

	// 删除 StreamGraph 中的一条边
	private void removeEdge(StreamEdge edge) {
		getSourceVertex(edge).getOutEdges().remove(edge);
		getTargetVertex(edge).getInEdges().remove(edge);
	}

	// 移除 StreamNode 中的一个节点，需要同时删除节点有关的所有边
	private void removeVertex(StreamNode toRemove) {
		Set<StreamEdge> edgesToRemove = new HashSet<>();

		edgesToRemove.addAll(toRemove.getInEdges());
		edgesToRemove.addAll(toRemove.getOutEdges());

		for (StreamEdge edge : edgesToRemove) {
			removeEdge(edge);
		}
		streamNodes.remove(toRemove.getId());
	}

	/**
	 * Gets the assembled {@link JobGraph} with a given job id.
	 */
	/**
	 * 生成 JobGraph
	 */
	@SuppressWarnings("deprecation")
	@Override
	public JobGraph getJobGraph(@Nullable JobID jobID) {
		// temporarily forbid checkpointing for iterative jobs
		// 临时禁止迭代任务的检查点
		if (isIterative() && checkpointConfig.isCheckpointingEnabled() && !checkpointConfig.isForceCheckpointing()) {
			throw new UnsupportedOperationException(
				"Checkpointing is currently not supported by default for iterative jobs, as we cannot guarantee exactly once semantics. "
					+ "State checkpoints happen normally, but records in-transit during the snapshot will be lost upon failure. "
					+ "\nThe user can force enable state checkpoints with the reduced guarantees by calling: env.enableCheckpointing(interval,true)");
		}

		return StreamingJobGraphGenerator.createJobGraph(this, jobID);
	}

	@Override
	public String getStreamingPlanAsJSON() {
		try {
			return new JSONGenerator(this).getJSON();
		}
		catch (Exception e) {
			throw new RuntimeException("JSON plan creation failed", e);
		}
	}

	@Override
	public void dumpStreamingPlanAsJSON(File file) throws IOException {
		PrintWriter pw = null;
		try {
			pw = new PrintWriter(new FileOutputStream(file), false);
			pw.write(getStreamingPlanAsJSON());
			pw.flush();

		} finally {
			if (pw != null) {
				pw.close();
			}
		}
	}
}
