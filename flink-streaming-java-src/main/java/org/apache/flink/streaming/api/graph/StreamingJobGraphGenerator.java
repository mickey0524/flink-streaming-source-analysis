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
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.operators.util.UserCodeObjectWrapper;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.optimizer.plantranslate.JobGraphGenerator;
import org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy;
import org.apache.flink.runtime.checkpoint.MasterTriggerRestoreHook;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.InputFormatVertex;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.WithMasterCheckpointHook;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.partitioner.RescalePartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.tasks.StreamIterationHead;
import org.apache.flink.streaming.runtime.tasks.StreamIterationTail;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.SerializedValue;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * The StreamingJobGraphGenerator converts a {@link StreamGraph} into a {@link JobGraph}.
 */
/**
 * StreamingJobGraphGenerator 将 StreamGraph 转换为 JobGraph
 */
@Internal
public class StreamingJobGraphGenerator {

	private static final Logger LOG = LoggerFactory.getLogger(StreamingJobGraphGenerator.class);

	// ------------------------------------------------------------------------

	public static JobGraph createJobGraph(StreamGraph streamGraph) {
		return createJobGraph(streamGraph, null);
	}

	public static JobGraph createJobGraph(StreamGraph streamGraph, @Nullable JobID jobID) {
		return new StreamingJobGraphGenerator(streamGraph, jobID).createJobGraph();
	}

	// ------------------------------------------------------------------------

	private final StreamGraph streamGraph;  // StreamGraphGenerator 生成的 StreamGraph

	private final Map<Integer, JobVertex> jobVertices;  // id -> JobVertex
	private final JobGraph jobGraph;  // createJobGraph 执行完毕之后得到的 JobGraph
	private final Collection<Integer> builtVertices;  // 已经构建的 JobVertex 的集合

	private final List<StreamEdge> physicalEdgesInOrder;  // 物理边集合（排除了 chain 内部的边），按创建顺序排序

	// 保存 chain 信息，部署的时候用来构建 OperatorChain startNodeId -> (currentNodeId -> StreamConfig)
	private final Map<Integer, Map<Integer, StreamConfig>> chainedConfigs;

	private final Map<Integer, StreamConfig> vertexConfigs;  // 所有节点的配置信息 id -> StreamConfig
	private final Map<Integer, String> chainedNames;  // 保存每个节点的名字 id -> chainedName

	private final Map<Integer, ResourceSpec> chainedMinResources;  // 保存每个节点的最小使用资源 id -> ResourceSpec
	private final Map<Integer, ResourceSpec> chainedPreferredResources;  // 保存每个节点的最大使用资源 id -> ResourceSpec

	private final StreamGraphHasher defaultStreamGraphHasher;  // 节点 hash code 生成器
	private final List<StreamGraphHasher> legacyStreamGraphHashers;  // 节点 hash code backword 生成器

	private StreamingJobGraphGenerator(StreamGraph streamGraph) {
		this(streamGraph, null);
	}

	private StreamingJobGraphGenerator(StreamGraph streamGraph, @Nullable JobID jobID) {
		this.streamGraph = streamGraph;
		this.defaultStreamGraphHasher = new StreamGraphHasherV2();
		this.legacyStreamGraphHashers = Arrays.asList(new StreamGraphUserHashHasher());

		this.jobVertices = new HashMap<>();
		this.builtVertices = new HashSet<>();
		this.chainedConfigs = new HashMap<>();
		this.vertexConfigs = new HashMap<>();
		this.chainedNames = new HashMap<>();
		this.chainedMinResources = new HashMap<>();
		this.chainedPreferredResources = new HashMap<>();
		this.physicalEdgesInOrder = new ArrayList<>();

		jobGraph = new JobGraph(jobID, streamGraph.getJobName());
	}

	/**
	 * 创建 JobGraph
	 */
	private JobGraph createJobGraph() {

		// make sure that all vertices start immediately
		// 确保所有的节点立即启动
		jobGraph.setScheduleMode(ScheduleMode.EAGER);

		// Generate deterministic hashes for the nodes in order to identify them across
		// submission if they didn't change.
		// 生成 StreamGraph 节点确定的 hashes，这是用于在提交任务的时候判断 StreamGraph 是否更改了
		// 保证如果提交的拓扑没有改变，则每次生成的 hash 都是一样的
		Map<Integer, byte[]> hashes = defaultStreamGraphHasher.traverseStreamGraphAndGenerateHashes(streamGraph);

		// Generate legacy version hashes for backwards compatibility
		// 为了以后能够兼容，生成 legacy version hashes
		List<Map<Integer, byte[]>> legacyHashes = new ArrayList<>(legacyStreamGraphHashers.size());
		for (StreamGraphHasher hasher : legacyStreamGraphHashers) {
			legacyHashes.add(hasher.traverseStreamGraphAndGenerateHashes(streamGraph));
		}

		// 用于存储 chain 内的 operator，key 是 chain 的 startNodeId，value 是 chain 中每一个 operator 的
		// (hash code，backword hash code) 组成的元组
		Map<Integer, List<Tuple2<byte[], byte[]>>> chainedOperatorHashes = new HashMap<>();

		// 最重要的函数，生成 JobVertex，JobEdge 等，并尽可能地将多个节点 chain 在一起
		setChaining(hashes, legacyHashes, chainedOperatorHashes);

		// 将每个 JobVertex 的入边集合也序列化到该 JobVertex 的 StreamConfig 中
		// (出边集合已经在 setChaining 的时候写入了)
		setPhysicalEdges();

		// 根据 group name，为每个 JobVertex 指定所属的 SlotSharingGroup
		// 以及针对 Iteration 的头尾设置 CoLocationGroup
		setSlotSharingAndCoLocation();

		// 配置 checkpoint，配置重启策略（不重启，还是固定延迟重启）
		configureCheckpointing();

		JobGraphGenerator.addUserArtifactEntries(streamGraph.getEnvironment().getCachedFiles(), jobGraph);

		// set the ExecutionConfig last when it has been finalized
		try {
			jobGraph.setExecutionConfig(streamGraph.getExecutionConfig());
		}
		catch (IOException e) {
			throw new IllegalConfigurationException("Could not serialize the ExecutionConfig." +
					"This indicates that non-serializable types (like custom serializers) were registered");
		}

		return jobGraph;
	}

	/**
	 * 设置物理边，将每个 JobVertex 的入边集合也序列化到该 JobVertex 的 StreamConfig 中
	 */
	private void setPhysicalEdges() {
		Map<Integer, List<StreamEdge>> physicalInEdgesInOrder = new HashMap<Integer, List<StreamEdge>>();

		for (StreamEdge edge : physicalEdgesInOrder) {
			int target = edge.getTargetId();

			List<StreamEdge> inEdges = physicalInEdgesInOrder.computeIfAbsent(target, k -> new ArrayList<>());

			inEdges.add(edge);
		}

		for (Map.Entry<Integer, List<StreamEdge>> inEdges : physicalInEdgesInOrder.entrySet()) {
			int vertex = inEdges.getKey();
			List<StreamEdge> edgeList = inEdges.getValue(); // JobVertex 所有入边的集合

			vertexConfigs.get(vertex).setInPhysicalEdges(edgeList);
		}
	}

	/**
	 * Sets up task chains from the source {@link StreamNode} instances.
	 *
	 * <p>This will recursively create all {@link JobVertex} instances.
	 */
	/**
	 * 从源节点开始设置工作链
	 * 本方法会递归生成所有的 JobVertex
	 */
	private void setChaining(Map<Integer, byte[]> hashes, List<Map<Integer, byte[]>> legacyHashes, Map<Integer, List<Tuple2<byte[], byte[]>>> chainedOperatorHashes) {
		for (Integer sourceNodeId : streamGraph.getSourceIDs()) {
			createChain(sourceNodeId, sourceNodeId, hashes, legacyHashes, 0, chainedOperatorHashes);
		}
	}

	/**
	 * 递归创建链
	 * @param startNodeId chain 开始的 StreamNode id
	 * @param currentNodeId 当前的 StreamNode id
	 * @param hashes 存储 StreamNode hash code 的 map
	 * @param legacyHashes 存储 backward hash code 的 list，可能由多个用户定义的 hash 函数
	 * @param chainIndex 当前节点位于 chain 中的下标
	 * @param chainedOperatorHashes 存储 chain 中操作符的 hash code 元组
	 */
	private List<StreamEdge> createChain(
			Integer startNodeId,
			Integer currentNodeId,
			Map<Integer, byte[]> hashes,
			List<Map<Integer, byte[]>> legacyHashes,
			int chainIndex,
			Map<Integer, List<Tuple2<byte[], byte[]>>> chainedOperatorHashes) {
		
		// 当前节点没有被处理过
		if (!builtVertices.contains(startNodeId)) {
			
			// 过渡用的出边集合，用来生成最终的 JobEdge，需要注意的是
			// transitiveOutEdges 不包括 chain 内部的边
			List<StreamEdge> transitiveOutEdges = new ArrayList<StreamEdge>();

			List<StreamEdge> chainableOutputs = new ArrayList<StreamEdge>();  // 存储链式的出边
			List<StreamEdge> nonChainableOutputs = new ArrayList<StreamEdge>();  // 存储不能链式的出边

			for (StreamEdge outEdge : streamGraph.getStreamNode(currentNodeId).getOutEdges()) {
				if (isChainable(outEdge, streamGraph)) {  // 本方法和 StreamGraphHasherV2.java 里的一样
					chainableOutputs.add(outEdge);
				} else {
					nonChainableOutputs.add(outEdge);
				}
			}

			for (StreamEdge chainable : chainableOutputs) {
				// 链式的边，源节点不变，chainIdx 加一
				transitiveOutEdges.addAll(
						createChain(startNodeId, chainable.getTargetId(), hashes, legacyHashes, chainIndex + 1, chainedOperatorHashes));
			}

			for (StreamEdge nonChainable : nonChainableOutputs) {
				transitiveOutEdges.add(nonChainable);
				// 非链式的边，源节点改变，chainIdx 归零
				createChain(nonChainable.getTargetId(), nonChainable.getTargetId(), hashes, legacyHashes, 0, chainedOperatorHashes);
			}

			List<Tuple2<byte[], byte[]>> operatorHashes =
				chainedOperatorHashes.computeIfAbsent(startNodeId, k -> new ArrayList<>());

			byte[] primaryHashBytes = hashes.get(currentNodeId);  // 获取当前节点的 hash code

			for (Map<Integer, byte[]> legacyHash : legacyHashes) {
				// 将 default hash code 和 backwords hash code 组成一个 Tuple 写入 operatorHashes
				operatorHashes.add(new Tuple2<>(primaryHashBytes, legacyHash.get(currentNodeId)));
			}

			// 生成当前节点的显示名，如："Keyed Aggregation -> Sink: Unnamed"
			chainedNames.put(currentNodeId, createChainedName(currentNodeId, chainableOutputs));
			chainedMinResources.put(currentNodeId, createChainedMinResources(currentNodeId, chainableOutputs));
			chainedPreferredResources.put(currentNodeId, createChainedPreferredResources(currentNodeId, chainableOutputs));

			// 如果当前节点是起始节点，则直接创建 JobVertex 并返回 StreamConfig，否则先创建一个空的 StreamConfig
			StreamConfig config = currentNodeId.equals(startNodeId)
					? createJobVertex(startNodeId, hashes, legacyHashes, chainedOperatorHashes)
					: new StreamConfig(new Configuration());

			// 设置 JobVertex 的 StreamConfig, 基本上是序列化 StreamNode 中的配置到 StreamConfig 中
			// 其中包括序列化器, StreamOperator, Checkpoint 等相关配置
			setVertexConfig(currentNodeId, config, chainableOutputs, nonChainableOutputs);

			if (currentNodeId.equals(startNodeId)) {
				// 链式的开头
				config.setChainStart();
				config.setChainIndex(0);
				config.setOperatorName(streamGraph.getStreamNode(currentNodeId).getOperatorName());
				// 我们也会把物理出边写入配置, 部署时会用到
				config.setOutEdgesInOrder(transitiveOutEdges);
				config.setOutEdges(streamGraph.getStreamNode(currentNodeId).getOutEdges());

				for (StreamEdge edge : transitiveOutEdges) {
					connect(startNodeId, edge);
				}
				// 将 chain 中所有子节点的 StreamConfig 写入到 headOfChain 节点的 CHAINED_TASK_CONFIG 配置中
				config.setTransitiveChainedTaskConfigs(chainedConfigs.get(startNodeId));

			} else {
				// 链式非开头的节点，将 config 加入链式开头节点的 config
				chainedConfigs.computeIfAbsent(startNodeId, k -> new HashMap<Integer, StreamConfig>());

				config.setChainIndex(chainIndex);
				StreamNode node = streamGraph.getStreamNode(currentNodeId);
				config.setOperatorName(node.getOperatorName());
				chainedConfigs.get(startNodeId).put(currentNodeId, config);
			}

			config.setOperatorID(new OperatorID(primaryHashBytes));

			if (chainableOutputs.isEmpty()) {
				config.setChainEnd();
			}
			// 返回连往chain外部的出边集合
			return transitiveOutEdges;

		} else {
			return new ArrayList<>();
		}
	}

	/**
	 * 创建链式的名字，因为 createChain 方法是递归调用的，所以出边的 name 存在于 chainedNames 内
	 * @param vertexID 当前节点
	 * @param chainedOutputs 当前节点的链式出边集合
	 */
	private String createChainedName(Integer vertexID, List<StreamEdge> chainedOutputs) {
		// 当前节点的名称
		String operatorName = streamGraph.getStreamNode(vertexID).getOperatorName();
		if (chainedOutputs.size() > 1) {
			List<String> outputChainedNames = new ArrayList<>();
			for (StreamEdge chainable : chainedOutputs) {
				outputChainedNames.add(chainedNames.get(chainable.getTargetId()));
			}
			return operatorName + " -> (" + StringUtils.join(outputChainedNames, ", ") + ")";
		} else if (chainedOutputs.size() == 1) {
			return operatorName + " -> " + chainedNames.get(chainedOutputs.get(0).getTargetId());
		} else {
			return operatorName;
		}
	}

	/**
	 * 创建链式的最小资源
	 */
	private ResourceSpec createChainedMinResources(Integer vertexID, List<StreamEdge> chainedOutputs) {
		ResourceSpec minResources = streamGraph.getStreamNode(vertexID).getMinResources();
		for (StreamEdge chainable : chainedOutputs) {
			minResources = minResources.merge(chainedMinResources.get(chainable.getTargetId()));
		}
		return minResources;
	}

	/**
	 * 创建链式的最多资源
	 */
	private ResourceSpec createChainedPreferredResources(Integer vertexID, List<StreamEdge> chainedOutputs) {
		ResourceSpec preferredResources = streamGraph.getStreamNode(vertexID).getPreferredResources();
		for (StreamEdge chainable : chainedOutputs) {
			preferredResources = preferredResources.merge(chainedPreferredResources.get(chainable.getTargetId()));
		}
		return preferredResources;
	}

	/**
	 * 创建 JobGraph 节点
	 */
	private StreamConfig createJobVertex(
			Integer streamNodeId,
			Map<Integer, byte[]> hashes,
			List<Map<Integer, byte[]>> legacyHashes,
			Map<Integer, List<Tuple2<byte[], byte[]>>> chainedOperatorHashes) {

		JobVertex jobVertex;
		StreamNode streamNode = streamGraph.getStreamNode(streamNodeId);

		byte[] hash = hashes.get(streamNodeId);

		if (hash == null) {
			throw new IllegalStateException("Cannot find node hash. " +
					"Did you generate them before calling this method?");
		}
		
		// hash code 生成 jobVertexId
		JobVertexID jobVertexId = new JobVertexID(hash);

		// backwords jobVertexId
		List<JobVertexID> legacyJobVertexIds = new ArrayList<>(legacyHashes.size());
		for (Map<Integer, byte[]> legacyHash : legacyHashes) {
			hash = legacyHash.get(streamNodeId);
			if (null != hash) {
				legacyJobVertexIds.add(new JobVertexID(hash));
			}
		}

		List<Tuple2<byte[], byte[]>> chainedOperators = chainedOperatorHashes.get(streamNodeId);
		List<OperatorID> chainedOperatorVertexIds = new ArrayList<>();
		List<OperatorID> userDefinedChainedOperatorVertexIds = new ArrayList<>();
		// 生成链中操作符的 id 和 backword id
		if (chainedOperators != null) {
			for (Tuple2<byte[], byte[]> chainedOperator : chainedOperators) {
				chainedOperatorVertexIds.add(new OperatorID(chainedOperator.f0));
				userDefinedChainedOperatorVertexIds.add(chainedOperator.f1 != null ? new OperatorID(chainedOperator.f1) : null);
			}
		}

		if (streamNode.getInputFormat() != null) {
			jobVertex = new InputFormatVertex(
					chainedNames.get(streamNodeId),
					jobVertexId,
					legacyJobVertexIds,
					chainedOperatorVertexIds,
					userDefinedChainedOperatorVertexIds);
			TaskConfig taskConfig = new TaskConfig(jobVertex.getConfiguration());
			taskConfig.setStubWrapper(new UserCodeObjectWrapper<Object>(streamNode.getInputFormat()));
		} else {
			jobVertex = new JobVertex(
					chainedNames.get(streamNodeId),  // job 节点的名字
					jobVertexId,  // job 节点的 id
					legacyJobVertexIds,  // job 节点 backword ids
					chainedOperatorVertexIds,  // chain 中所有操作符的 id
					userDefinedChainedOperatorVertexIds);  // chain 中所有操作符的 backword ids
		}

		// 设置节点的最小资源和最大资源
		jobVertex.setResources(chainedMinResources.get(streamNodeId), chainedPreferredResources.get(streamNodeId));

		// 设置 Task 类
		jobVertex.setInvokableClass(streamNode.getJobVertexClass());

		// 设置并行度
		int parallelism = streamNode.getParallelism();

		if (parallelism > 0) {
			jobVertex.setParallelism(parallelism);
		} else {
			parallelism = jobVertex.getParallelism();
		}

		// 设置最大并行度
		jobVertex.setMaxParallelism(streamNode.getMaxParallelism());

		if (LOG.isDebugEnabled()) {
			LOG.debug("Parallelism set: {} for {}", parallelism, streamNodeId);
		}

		// TODO: inherit InputDependencyConstraint from the head operator
		jobVertex.setInputDependencyConstraint(streamGraph.getExecutionConfig().getDefaultInputDependencyConstraint());

		jobVertices.put(streamNodeId, jobVertex);
		builtVertices.add(streamNodeId);
		jobGraph.addVertex(jobVertex);

		return new StreamConfig(jobVertex.getConfiguration());
	}

	@SuppressWarnings("unchecked")
	/**
	 * 设置节点 config
	 * 一个 node chains，除了 headOfChain node 会生成对应的 JobVertex
	 * 其余的 nodes 都是以序列化的形式写入到 StreamConfig 中
	 * 并保存到 headOfChain 的 CHAINED_TASK_CONFIG 配置项中。直到部署时，才会取出并生成对应的 ChainOperators
	 */
	private void setVertexConfig(Integer vertexID, StreamConfig config,
			List<StreamEdge> chainableOutputs, List<StreamEdge> nonChainableOutputs) {

		StreamNode vertex = streamGraph.getStreamNode(vertexID);

		config.setVertexID(vertexID);
		config.setBufferTimeout(vertex.getBufferTimeout());

		config.setTypeSerializerIn1(vertex.getTypeSerializerIn1());
		config.setTypeSerializerIn2(vertex.getTypeSerializerIn2());
		config.setTypeSerializerOut(vertex.getTypeSerializerOut());

		// iterate edges, find sideOutput edges create and save serializers for each outputTag type
		// 遍历所有的边，找到 sideOutput 的边，为每一个 outputTag type 创建保存序列器
		for (StreamEdge edge : chainableOutputs) {
			if (edge.getOutputTag() != null) {
				config.setTypeSerializerSideOut(
					edge.getOutputTag(),
					edge.getOutputTag().getTypeInfo().createSerializer(streamGraph.getExecutionConfig())
				);
			}
		}
		for (StreamEdge edge : nonChainableOutputs) {
			if (edge.getOutputTag() != null) {
				config.setTypeSerializerSideOut(
						edge.getOutputTag(),
						edge.getOutputTag().getTypeInfo().createSerializer(streamGraph.getExecutionConfig())
				);
			}
		}

		config.setStreamOperator(vertex.getOperator());
		config.setOutputSelectors(vertex.getOutputSelectors());

		config.setNumberOfOutputs(nonChainableOutputs.size());
		config.setNonChainedOutputs(nonChainableOutputs);
		config.setChainedOutputs(chainableOutputs);

		config.setTimeCharacteristic(streamGraph.getEnvironment().getStreamTimeCharacteristic());

		final CheckpointConfig checkpointCfg = streamGraph.getCheckpointConfig();

		config.setStateBackend(streamGraph.getStateBackend());
		config.setCheckpointingEnabled(checkpointCfg.isCheckpointingEnabled());
		if (checkpointCfg.isCheckpointingEnabled()) {
			config.setCheckpointMode(checkpointCfg.getCheckpointingMode());
		}
		else {
			// the "at-least-once" input handler is slightly cheaper (in the absence of checkpoints),
			// so we use that one if checkpointing is not enabled
			// "at-least-once" 处理方式更为简单，当没有设置检查模式的时候，我们使用该方式
			config.setCheckpointMode(CheckpointingMode.AT_LEAST_ONCE);
		}
		config.setStatePartitioner(0, vertex.getStatePartitioner1());
		config.setStatePartitioner(1, vertex.getStatePartitioner2());
		config.setStateKeySerializer(vertex.getStateKeySerializer());

		Class<? extends AbstractInvokable> vertexClass = vertex.getJobVertexClass();

		if (vertexClass.equals(StreamIterationHead.class)
				|| vertexClass.equals(StreamIterationTail.class)) {
			config.setIterationId(streamGraph.getBrokerID(vertexID));
			config.setIterationWaitTime(streamGraph.getLoopTimeout(vertexID));
		}

		vertexConfigs.put(vertexID, config);
	}

	/**
	 * 连接两个 JobVertex (chain 的第一个节点和一个出边（不在 chain 内）相连的节点)
	 */
	private void connect(Integer headOfChain, StreamEdge edge) {
		// 物理边的顺序
		physicalEdgesInOrder.add(edge);

		Integer downStreamvertexID = edge.getTargetId();

		JobVertex headVertex = jobVertices.get(headOfChain);
		JobVertex downStreamVertex = jobVertices.get(downStreamvertexID);

		StreamConfig downStreamConfig = new StreamConfig(downStreamVertex.getConfiguration());

		// 出节点的入度 + 1
		downStreamConfig.setNumberOfInputs(downStreamConfig.getNumberOfInputs() + 1);

		StreamPartitioner<?> partitioner = edge.getPartitioner();
		JobEdge jobEdge;
		// 当 partitioner 是 ForwardPartitioner 或者 RescalePartitioner 的时候
		// 上游 SubTask 节点仅仅会连接到部分下游的 SubTask 节点 (DistributionPattern.POINTWISE)
		// 其他的 Partitioner 上游 SubTask 节点会连接到所有下游的 SubTask 节点 (DistributionPattern.ALL_TO_ALL)
		if (partitioner instanceof ForwardPartitioner || partitioner instanceof RescalePartitioner) {
			jobEdge = downStreamVertex.connectNewDataSetAsInput(
				headVertex,
				DistributionPattern.POINTWISE,
				ResultPartitionType.PIPELINED_BOUNDED);
		} else {
			jobEdge = downStreamVertex.connectNewDataSetAsInput(
					headVertex,
					DistributionPattern.ALL_TO_ALL,
					ResultPartitionType.PIPELINED_BOUNDED);
		}
		// set strategy name so that web interface can show it.
		jobEdge.setShipStrategyName(partitioner.toString());

		if (LOG.isDebugEnabled()) {
			LOG.debug("CONNECTED: {} - {} -> {}", partitioner.getClass().getSimpleName(),
					headOfChain, downStreamvertexID);
		}
	}

	/**
	 * 注释见 StreamGraphHasherV2.java
	 */
	public static boolean isChainable(StreamEdge edge, StreamGraph streamGraph) {
		StreamNode upStreamVertex = streamGraph.getSourceVertex(edge);
		StreamNode downStreamVertex = streamGraph.getTargetVertex(edge);

		StreamOperator<?> headOperator = upStreamVertex.getOperator();
		StreamOperator<?> outOperator = downStreamVertex.getOperator();

		return downStreamVertex.getInEdges().size() == 1
				&& outOperator != null
				&& headOperator != null
				&& upStreamVertex.isSameSlotSharingGroup(downStreamVertex)
				&& outOperator.getChainingStrategy() == ChainingStrategy.ALWAYS
				&& (headOperator.getChainingStrategy() == ChainingStrategy.HEAD ||
					headOperator.getChainingStrategy() == ChainingStrategy.ALWAYS)
				&& (edge.getPartitioner() instanceof ForwardPartitioner)
				&& upStreamVertex.getParallelism() == downStreamVertex.getParallelism()
				&& streamGraph.isChainingEnabled();
	}

	// 根据 group name，为每个 JobVertex 指定所属的 SlotSharingGroup
	// 以及针对 Iteration 的头尾设置 CoLocationGroup
	private void setSlotSharingAndCoLocation() {
		final HashMap<String, SlotSharingGroup> slotSharingGroups = new HashMap<>();
		final HashMap<String, Tuple2<SlotSharingGroup, CoLocationGroup>> coLocationGroups = new HashMap<>();

		for (Entry<Integer, JobVertex> entry : jobVertices.entrySet()) {

			final StreamNode node = streamGraph.getStreamNode(entry.getKey());
			final JobVertex vertex = entry.getValue();

			// configure slot sharing group
			// 配置 slot sharing group
			final String slotSharingGroupKey = node.getSlotSharingGroup();
			final SlotSharingGroup sharingGroup;

			if (slotSharingGroupKey != null) {
				sharingGroup = slotSharingGroups.computeIfAbsent(
						slotSharingGroupKey, (k) -> new SlotSharingGroup());
				vertex.setSlotSharingGroup(sharingGroup);
			} else {
				sharingGroup = null;
			}

			// configure co-location constraint
			final String coLocationGroupKey = node.getCoLocationGroup();
			if (coLocationGroupKey != null) {
				if (sharingGroup == null) {
					throw new IllegalStateException("Cannot use a co-location constraint without a slot sharing group");
				}

				Tuple2<SlotSharingGroup, CoLocationGroup> constraint = coLocationGroups.computeIfAbsent(
						coLocationGroupKey, (k) -> new Tuple2<>(sharingGroup, new CoLocationGroup()));

				if (constraint.f0 != sharingGroup) {
					throw new IllegalStateException("Cannot co-locate operators from different slot sharing groups");
				}

				vertex.updateCoLocationGroup(constraint.f1);
			}
		}

		for (Tuple2<StreamNode, StreamNode> pair : streamGraph.getIterationSourceSinkPairs()) {

			CoLocationGroup ccg = new CoLocationGroup();

			JobVertex source = jobVertices.get(pair.f0.getId());
			JobVertex sink = jobVertices.get(pair.f1.getId());

			ccg.addVertex(source);
			ccg.addVertex(sink);
			source.updateCoLocationGroup(ccg);
			sink.updateCoLocationGroup(ccg);
		}

	}

	// 配置 checkpoint，配置重启策略（不重启，还是固定延迟重启）
	private void configureCheckpointing() {
		CheckpointConfig cfg = streamGraph.getCheckpointConfig();

		long interval = cfg.getCheckpointInterval();
		if (interval > 0) {
			ExecutionConfig executionConfig = streamGraph.getExecutionConfig();
			// propagate the expected behaviour for checkpoint errors to task.
			executionConfig.setFailTaskOnCheckpointError(cfg.isFailOnCheckpointingErrors());
		} else {
			// interval of max value means disable periodic checkpoint
			// interval 设为 Long.MAX_VALUE 意味着 disable 周期性的检查
			interval = Long.MAX_VALUE;
		}

		//  --- configure the participating vertices ---
		// 配置参与的节点
		// collect the vertices that receive "trigger checkpoint" messages.
		// currently, these are all the sources
		// 收集收到 "trigger checkpoint" 消息的节点
		// 目前，这些节点都是 sources
		List<JobVertexID> triggerVertices = new ArrayList<>();

		// collect the vertices that need to acknowledge the checkpoint
		// currently, these are all vertices
		// 收集需要 ack checkpoint 的节点
		// 目前，所有的节点都是
		List<JobVertexID> ackVertices = new ArrayList<>(jobVertices.size());

		// collect the vertices that receive "commit checkpoint" messages
		// currently, these are all vertices
		// 收集收到 "commit checkpoint" 消息的节点
		// 目前，所有的节点都是
		List<JobVertexID> commitVertices = new ArrayList<>(jobVertices.size());

		for (JobVertex vertex : jobVertices.values()) {
			// 没有输入即为 InputVertex
			if (vertex.isInputVertex()) {
				triggerVertices.add(vertex.getID());
			}
			commitVertices.add(vertex.getID());
			ackVertices.add(vertex.getID());
		}

		//  --- configure options ---
		// 配置选项
		// CheckpointRetentionPolicy 指的是当 job 终止，检查点在什么时候保留
		// RETAIN_ON_CANCELLATION 当 job 取消和失败的时候都要保留
		// RETAIN_ON_FAILURE 当 job 失败的时候保留
		// NEVER_RETAIN_AFTER_TERMINATION 不保留
		CheckpointRetentionPolicy retentionAfterTermination;
		if (cfg.isExternalizedCheckpointsEnabled()) {
			CheckpointConfig.ExternalizedCheckpointCleanup cleanup = cfg.getExternalizedCheckpointCleanup();
			// Sanity check
			if (cleanup == null) {
				throw new IllegalStateException("Externalized checkpoints enabled, but no cleanup mode configured.");
			}
			retentionAfterTermination = cleanup.deleteOnCancellation() ?
					CheckpointRetentionPolicy.RETAIN_ON_FAILURE :
					CheckpointRetentionPolicy.RETAIN_ON_CANCELLATION;
		} else {
			retentionAfterTermination = CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION;
		}

		CheckpointingMode mode = cfg.getCheckpointingMode();

		boolean isExactlyOnce;
		if (mode == CheckpointingMode.EXACTLY_ONCE) {
			isExactlyOnce = true;
		} else if (mode == CheckpointingMode.AT_LEAST_ONCE) {
			isExactlyOnce = false;
		} else {
			throw new IllegalStateException("Unexpected checkpointing mode. " +
				"Did not expect there to be another checkpointing mode besides " +
				"exactly-once or at-least-once.");
		}

		//  --- configure the master-side checkpoint hooks ---
		// 配置主端检查点回调

		final ArrayList<MasterTriggerRestoreHook.Factory> hooks = new ArrayList<>();

		for (StreamNode node : streamGraph.getStreamNodes()) {
			StreamOperator<?> op = node.getOperator();
			if (op instanceof AbstractUdfStreamOperator) {
				Function f = ((AbstractUdfStreamOperator<?, ?>) op).getUserFunction();

				if (f instanceof WithMasterCheckpointHook) {
					hooks.add(new FunctionMasterCheckpointHookFactory((WithMasterCheckpointHook<?>) f));
				}
			}
		}

		// because the hooks can have user-defined code, they need to be stored as
		// eagerly serialized values
		// 因为 hooks 可能有用户定义的代码，因此需要被序列化
		final SerializedValue<MasterTriggerRestoreHook.Factory[]> serializedHooks;
		if (hooks.isEmpty()) {
			serializedHooks = null;
		} else {
			try {
				MasterTriggerRestoreHook.Factory[] asArray =
						hooks.toArray(new MasterTriggerRestoreHook.Factory[hooks.size()]);
				serializedHooks = new SerializedValue<>(asArray);
			}
			catch (IOException e) {
				throw new FlinkRuntimeException("Trigger/restore hook is not serializable", e);
			}
		}

		// because the state backend can have user-defined code, it needs to be stored as
		// eagerly serialized value
		// 因为 state backend 可能有用户定义的代码，因此需要被序列化
		final SerializedValue<StateBackend> serializedStateBackend;
		if (streamGraph.getStateBackend() == null) {
			serializedStateBackend = null;
		} else {
			try {
				serializedStateBackend =
					new SerializedValue<StateBackend>(streamGraph.getStateBackend());
			}
			catch (IOException e) {
				throw new FlinkRuntimeException("State backend is not serializable", e);
			}
		}

		//  --- done, put it all together ---

		JobCheckpointingSettings settings = new JobCheckpointingSettings(
			triggerVertices,
			ackVertices,
			commitVertices,
			new CheckpointCoordinatorConfiguration(
				interval,
				cfg.getCheckpointTimeout(),
				cfg.getMinPauseBetweenCheckpoints(),
				cfg.getMaxConcurrentCheckpoints(),
				retentionAfterTermination,
				isExactlyOnce),
			serializedStateBackend,
			serializedHooks);

		jobGraph.setSnapshotSettings(settings);
	}
}
