# flink 的 JobGraph

这篇文章我们来讲解一下 flink 的 JobGraph

## JobStream 计算的入口

从上一篇文章，我们知道，flink 的 execute 方法首先会计算得到 StreamGraph，而 JobGraph 就是通过计算 StreamGraph 得到的

```java
public JobExecutionResult execute(String jobName) throws Exception {
	// 第一步，生成 StreamGraph
	StreamGraph streamGraph = getStreamGraph();

	// 第二步，生成 JobGraph
	JobGraph jobGraph = streamGraph.getJobGraph();
	...
}
```

## StreamGraphHasher

在介绍 JobStream 之前，我们先来介绍一下 StreamGraphHasher，StreamGraphHasher 会遍历生成 StreamGraph 节点的 hash 值，这是用于在提交任务的时候判断 StreamGraph 是否更改了，如果提交的拓扑没有改变，则每次生成的 hash 都是一样的

StreamGraphHasher 是一个接口

```java
public interface StreamGraphHasher {

	/**
	 * 返回一个 map，为每一个 StreamNode 生成一个 hash
	 * hash 是用于 JobVertexID，为了在 job 提交的过程中判断节点是否发生了变化
	 */
	Map<Integer, byte[]> traverseStreamGraphAndGenerateHashes(StreamGraph streamGraph);
}
```

### StreamGraphUserHashHasher

StreamGraphUserHashHasher 实现了 StreamGraphHasher 接口，工作在用户提供 hash 的场景下，当我们想要有选择的设置 hash 的时候，StreamGraphHasher 十分有用，这也给我们提供了向下兼容的能力，防止不同版本产生 hash 的机制不一致

在 StreamTransformation 存在一个字段 userProvidedNodeHash，在 StreamGraphGenerator 的 transform 方法中，当 userProvidedNodeHash 字段不为 null 的时候，会被写入对应的 StreamNode 的 userHash 字段中

```java
private String userProvidedNodeHash;

...

if (transform.getUserProvidedNodeHash() != null) {
	streamGraph.setTransformationUserHash(transform.getId(), transform.getUserProvidedNodeHash());
}
```

StreamGraphUserHashHasher 遍历 StreamGraph 所有的节点，获取 StreamNode 的 userHash，返回一个 (StreamNodeId, userHash) 的哈希表

```java
public class StreamGraphUserHashHasher implements StreamGraphHasher {

	@Override
	public Map<Integer, byte[]> traverseStreamGraphAndGenerateHashes(StreamGraph streamGraph) {
		HashMap<Integer, byte[]> hashResult = new HashMap<>();
		for (StreamNode streamNode : streamGraph.getStreamNodes()) {

			String userHash = streamNode.getUserHash();

			if (null != userHash) {
				hashResult.put(streamNode.getId(), StringUtils.hexStringToByte(userHash));
			}
		}

		return hashResult;
	}
}
```

### StreamGraphHasherV2

StreamGraphHasherV2 是 flink 1.2 版本之后的 StreamGraphHasher，同样实现了 StreamGraphHasher 接口。在 StreamGraphHasherV2 中，StreamNode 的 hash 生成方式就更为复杂

traverseStreamGraphAndGenerateHashes 方法首先获取 StreamGraph 中所有的源头节点，然后将其排序，这样每次遍历的顺序都是确定相同的。visited 用来保存当前 id 的 StreamNode 是否访问过，remaining 用来充当 BFS 遍历的队列，首先将所有的源头节点加入 visited 和 remaining，开始遍历

generateNodeHash 方法是真正生成 hash 值的方法，当返回 false 的时候，说明当前节点不满足生成 hash 值的条件，我们将其移出 visited 集合，随后再访问，当返回 true 的时候，将当前节点的所有出边的目标节点加入队列

```java
public Map<Integer, byte[]> traverseStreamGraphAndGenerateHashes(StreamGraph streamGraph) {
	// hash 函数用于生成 hash
	final HashFunction hashFunction = Hashing.murmur3_128(0);
	final Map<Integer, byte[]> hashes = new HashMap<>();

	Set<Integer> visited = new HashSet<>();
	Queue<StreamNode> remaining = new ArrayDeque<>();

	// 我们需要让源节点顺序是确定的，如果源节点 id 没有按相同的顺序返回，这意味着提交相同
	// 的程序有可能会得到不同的遍历，会破坏 hash 分配的确定性
	List<Integer> sources = new ArrayList<>();
	for (Integer sourceNodeId : streamGraph.getSourceIDs()) {
		sources.add(sourceNodeId);
	}
	Collections.sort(sources);

	// 按 BFS 遍历图，需要知道的是图不是一颗树
	// 因此多条路径到一个节点是可能存在的

	for (Integer sourceNodeId : sources) {
		remaining.add(streamGraph.getStreamNode(sourceNodeId));
		visited.add(sourceNodeId);
	}

	StreamNode currentNode;
	while ((currentNode = remaining.poll()) != null) {
		// 生成 hash code。因为对每个节点来说存在多条路径，我们可能没有生成 hash code 所需
		// 的所有 inputs
		if (generateNodeHash(currentNode, hashFunction, hashes, streamGraph.isChainingEnabled(), streamGraph)) {
			for (StreamEdge outEdge : currentNode.getOutEdges()) {
				StreamNode child = streamGraph.getTargetVertex(outEdge);

				if (!visited.contains(child.getId())) {
					remaining.add(child);
					visited.add(child.getId());
				}
			}
		} else {
			// 我们会在随后重新访问该节点
			visited.remove(currentNode.getId());
		}
	}

	return hashes;
}
```

generateNodeHash 方法根据 StreamNode 的 transformationUID 字段是否为 null 选取不同的生成方式

#### transformationUID 不为 null 

当 transformationUID 不为 null，根据 transformationUID 生成 hash。
在 StreamTransformation 存在一个字段 uid，在 StreamGraphGenerator 的 transform 方法中，当 uid 字段不为 null 的时候，会被写入对应的 StreamNode 的 transformationUID 字段中

```java
private String uid;

...

if (transform.getUid() != null) {
	streamGraph.setTransformationUID(transform.getId(), transform.getUid());
}
```

可以看到，直接将 transformationUID 写入 hash 生成器获取 hash code，再将 hash code 与之前访问节点的 hash code 比较，查看是否碰撞，最后将 hash code 写入 map

```java
Hasher hasher = hashFunction.newHasher();  // 新生成一个 hash code
byte[] hash = generateUserSpecifiedHash(node, hasher);

// 检查 hash 冲突
for (byte[] previousHash : hashes.values()) {
	if (Arrays.equals(previousHash, hash)) {
		// 如果冲突的话，很大概率是由于重复的 uid 导致的
		throw new IllegalArgumentException("Hash collision on user-specified ID " +
				"\"" + userSpecifiedHash + "\". " +
				"Most likely cause is a non-unique ID. Please check that all IDs " +
				"specified via `uid(String)` are unique.");
	}
}

if (hashes.put(node.getId(), hash) != null) {
	// Sanity check
	throw new IllegalStateException("Unexpected state. Tried to add node hash " +
			"twice. This is probably a bug in the JobGraph generator.");
}

return true;

...

private byte[] generateUserSpecifiedHash(StreamNode node, Hasher hasher) {
	hasher.putString(node.getTransformationUID(), Charset.forName("UTF-8"));

	return hasher.hash().asBytes();
}
```

#### transformationUID 为 null 

当 transformationUID 为 null 的时候，根据节点的上游节点的 hash code、节点自身的属性以及节点的链式下游数量来生成当前节点的 hash code

因为需要依赖于所有的上游节点，所以需要先判断所有的上游节点是否已经生成了 hash code

```java
for (StreamEdge inEdge : node.getInEdges()) {
	// 如果输入节点还没有被访问，当所有的输入节点都被访问过且 hash code 都被设置之后
	// 当前节点会被重新访问
	if (!hashes.containsKey(inEdge.getSourceId())) {
		return false;
	}
}
```

接下来看看 hash code 是如何生成的

```java
// 将节点自身的属性加入 hash
hasher.putInt(hashes.size());

// 将链接的后续节点加入 hash
for (StreamEdge outEdge : node.getOutEdges()) {
	if (isChainable(outEdge, isChainingEnabled, streamGraph)) {
		hasher.putInt(hashes.size());
	}
}

byte[] hash = hasher.hash().asBytes();

// 确保进入该方法的时候，所有的 input nodes 都有自己的 hash code
for (StreamEdge inEdge : node.getInEdges()) {
	byte[] otherHash = hashes.get(inEdge.getSourceId());

	// 将输入节点的 hash 加入计算
	for (int j = 0; j < hash.length; j++) {
		hash[j] = (byte) (hash[j] * 37 ^ otherHash[j]);
	}
}
```

首先，将节点自身的属性加入 hash，需要注意，我们使用 hashes 当前的 size 作为 id，我们不能使用 node 的 id，因为它是由一个静态 counter 分配 id 的，这会导致相同的程序得到不同的 hashes，例如

```java
// 如下所示：
// 范例1：A.id = 1  B.id = 2
// DataStream<String> A = ...
// DataStream<String> B = ...
// A.union(B).print();
// 范例2：A.id = 2  B.id = 1
// DataStream<String> B = ...
// DataStream<String> A = ...
// A.union(B).print();
// 上面的两个 job 是完全一样的拓扑，但是 source 的 id 却不一样
```

然后遍历所有节点的链式后缀节点，同样加入 hashes 当前的 size，最后，我们将本节点当前的 hash 和上游节点的 hash 按位计算，得到 hash code

#### isChainable

isChainable 方法用来判断两个节点是否是链式连接在一起的

```java
private boolean isChainable(StreamEdge edge, boolean isChainingEnabled, StreamGraph streamGraph) {
	StreamNode upStreamVertex = streamGraph.getSourceVertex(edge);  // 获取边的源节点
	StreamNode downStreamVertex = streamGraph.getTargetVertex(edge);  // 获取边的目标节点

	StreamOperator<?> headOperator = upStreamVertex.getOperator();  // 获取源头节点的操作符
	StreamOperator<?> outOperator = downStreamVertex.getOperator();  // 获取目标节点的操作符

	return downStreamVertex.getInEdges().size() == 1  // 目标节点的入度为 1
			&& outOperator != null  // 目标节点操作符不能为空
			&& headOperator != null  // 源节点操作符不能为空
			&& upStreamVertex.isSameSlotSharingGroup(downStreamVertex)  // 源头节点和目标节点的 slot sharing group 相同
			&& outOperator.getChainingStrategy() == ChainingStrategy.ALWAYS
			&& (headOperator.getChainingStrategy() == ChainingStrategy.HEAD || // HEAD 模式允许后续节点链式连接
			headOperator.getChainingStrategy() == ChainingStrategy.ALWAYS)
			&& (edge.getPartitioner() instanceof ForwardPartitioner)
			&& upStreamVertex.getParallelism() == downStreamVertex.getParallelism()
			&& isChainingEnabled;
}
```

## StreamingJobGraphGenerator

streamGraph 的 getJobGraph 方法会调用 `StreamingJobGraphGenerator.createJobGraph(this, jobID)`，根据 streamGraph 创建 jobGraph

### 重要属性

```java
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
```

### createJobGraph

createJobGraph 中首先调用前文讲到过的 StreamGraphUserHashHasher 和 StreamGraphHasherV2 的 traverseStreamGraphAndGenerateHashes 方法，获取 StreamNode 的 hash code，然后调用 setChaining 方法，生成 JobVertex，JobEdge 等，并尽可能地将多个节点 chain 在一起

```java
// 生成 StreamGraph 节点确定的 hashes，这是用于在提交任务的时候判断 StreamGraph 是否更改了
// 保证如果提交的拓扑没有改变，则每次生成的 hash 都是一样的
Map<Integer, byte[]> hashes = defaultStreamGraphHasher.traverseStreamGraphAndGenerateHashes(streamGraph);

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
```

### setChaining

setChaining 方法遍历 streamGraph 中的所有的源节点，调用 createChain 方法

```java
private void setChaining(Map<Integer, byte[]> hashes, List<Map<Integer, byte[]>> legacyHashes, Map<Integer, List<Tuple2<byte[], byte[]>>> chainedOperatorHashes) {
	for (Integer sourceNodeId : streamGraph.getSourceIDs()) {
		createChain(sourceNodeId, sourceNodeId, hashes, legacyHashes, 0, chainedOperatorHashes);
	}
}
```

### createChain

createChain 会被递归的调用，尽可能的将多个节点 chain 在一起，createChain 会遍历当前节点的所有出边，调用 `isChainable(和前文 hash 计算处的代码完全相同)` 判断边的源头节点和目标节点之间能否链式连接，能够 chain 的边加入 chainableOutputs，反之加入 nonChainableOutputs

对于 chainableOutputs 的边，取出边的目标节点递归调用 createChain，对于 nonChainableOutputs 的边，说明边的目标节点和源头节点不可能位于同一个 chain 中，重新调用 createChain，并将边加入 transitiveOutEdges 中

执行完上述操作后，其实图中已经虚拟的存在许多 chain 了，那么就会存在 headChain(链头操作符)，当 currentNodeId 等于 startNodeId 的时候，我们认为找到了一个 headChain，调用 createJobVertex 方法创建 jobGraph 中的一个节点

chainedConfigs 中存放着 chain 的配置，chain 是一个 `Map<Integer, Map<Integer, StreamConfig>>` 类型的 map，k 代表 headChain，v 也是一个 map，内部 map 的 k 是 chain 中的操作符 id，v 代表该操作符的配置，createChain 会调用 setVertexConfig 方法生成每个操作符的配置，然后每个操作符的配置都会被写 headChain 对应的 map 中，最后组成 chainedConfigs

和 StreamGraph 一样，JobGraph 中的节点也需要连接边，transitiveOutEdges 的边视为 chain 与链外的边，createChain 会调用 connect 方法在 headChain 和 transitiveOutEdges 边中的目标节点之间连接边

```java
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
```

### createJobVertex

createJobVertex 根据传入的 streamNodeId 获取两个 hash 生成器给出的 hash code，并根据 hash code 得到 JobVertexID，同时，我们知道 jobGraph 的节点可能包括多个 streamGraph 中的节点，因此 createJobVertex 方法会去获取每个操作符的 hash code，然后生成对应的 OperatorID，最后，创建 jobVertex

```java
private StreamConfig createJobVertex(
		Integer streamNodeId,
		Map<Integer, byte[]> hashes,
		List<Map<Integer, byte[]>> legacyHashes,
		Map<Integer, List<Tuple2<byte[], byte[]>>> chainedOperatorHashes) {

	JobVertex jobVertex;
	StreamNode streamNode = streamGraph.getStreamNode(streamNodeId);

	byte[] hash = hashes.get(streamNodeId);

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
```

### connect

connect 连接两个 JobVertex (chain 的第一个节点和一个出边（不在 chain 内）相连的节点)，当边上的 StreamPartitioner 是 ForwardPartitioner 或 RescalePartitioner 的时候，上游 SubTask 节点仅仅会连接到部分下游的 SubTask 节点，选用 DistributionPattern.POINTWISE 模式，其他的 Partitioner 上游 SubTask 节点会连接到所有下游的 SubTask 节点，选用 DistributionPattern.ALL\_TO\_ALL，大家可以结合[flink 中的 StreamPartitioner](./docs/flink-stream-partitioner.md)看看，ForwardPartitioner 的 selectChannel 方法返回 0，RescalePartitioner 的 selectChannel 方法轮询从 0 开始（和 RescalePartitioner 非常类似的 RebalancePartitioner selectChannel 方法轮询从随机下标开始）就是这里决定的
 
```java
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
```

## 总结

这篇文章，我们给大家介绍了一下 JobGraph 是如何生成的，StreamGraph 和。JobGraph 都是在 client 处生成的，大家可以结合起来看看，希望对大家有所帮助
