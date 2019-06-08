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

```
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

🚧 Under Construction

