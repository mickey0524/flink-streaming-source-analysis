# flink 的 StreamGraph

这篇文章我们来讲一下 flink 的 StreamGraph，在前面的文章中，多次提到了 StreamGraph 和 transformation 树，今天我们就来一探究竟

在正文开始之前，需要先介绍几个类

* StreamGraphGenerator：StreamGraph 的生成器
* StreamGraph：flink 中代表流拓扑的类
* StreamNode：StreamGraph 中的节点，实质是包含 StreamOperator 的 transformation
* StreamEdge：StreamGraph 中连接 StreamNode 的边，select，sideOutput，partition 都会成为 StreamEdge 的属性

## StreamGraph 计算的入口

当我们编写完 flink 代码之后，我们都需要调用 `env.execute` 来启动 flink，execute 方法中首先会执行 `StreamGraph streamGraph = getStreamGraph()` 得到 StreamGraph，从👇可以看到，getStreamGraph 会将 StreamExecutionEnvironment 和 transformations 传入 `StreamGraphGenerator.generate` 中，计算得到 StreamGraph

```java
public StreamGraph getStreamGraph() {
	if (transformations.size() <= 0) {
		throw new IllegalStateException("No operators defined in streaming topology. Cannot execute.");
	}
	return StreamGraphGenerator.generate(this, transformations);
}
```

接下来我们看看 transformations 是如何收集 StreamTransformation 的，StreamExecutionEnvironment 中提供了一个 addOperator 方法，
addOperator 方法用于将 StreamTransformation 加入 transformations 列表中，addOperator 会在 DataStream/KeyedStream 的 transform 方法中被调用

addOperator 函数

```java
public void addOperator(StreamTransformation<?> transformation) {
	Preconditions.checkNotNull(transformation, "transformation must not be null.");
	this.transformations.add(transformation);
}
```

DataStream 中的 transform 函数，根据传入的 OneInputStreamOperator 生成 OneInputTransformation，并将 OneInputTransformation 加入 transformation 列表

```java
public <R> SingleOutputStreamOperator<R> transform(String operatorName, TypeInformation<R> outTypeInfo, OneInputStreamOperator<T, R> operator) {

	OneInputTransformation<T, R> resultTransform = new OneInputTransformation<>(
			this.transformation,
			operatorName,
			operator,
			outTypeInfo,
			environment.getParallelism());

	SingleOutputStreamOperator<R> returnStream = new SingleOutputStreamOperator(environment, resultTransform);

	// 给执行环境的 transformations 加入一个 算子
	getExecutionEnvironment().addOperator(resultTransform);

	return returnStream;
}
```

## StreamGraphGenerator 类

### 重要属性和构造函数

```java
// 拓扑图
private final StreamGraph streamGraph;

// 保存已经执行过的 Transforms，这非常有必要，因为可能会出现循环，比如 feedback edges
private Map<StreamTransformation<?>, Collection<Integer>> alreadyTransformed;

private StreamGraphGenerator(StreamExecutionEnvironment env) {
	this.streamGraph = new StreamGraph(env);
	this.streamGraph.setChaining(env.isChainingEnabled());
	this.streamGraph.setStateBackend(env.getStateBackend());
	this.env = env;
	this.alreadyTransformed = new HashMap<>();
}
```

### 启动方法

前面我们讲过，execute 方法会调用 StreamGraphGenerator 的 generate 方法，generate 是一个静态方法，会创建一个 StreamGraphGenerator 实例，然后调用 generateInternal 方法遍历 StreamTransformations 列表

我们可以看到 transform 方法接收一个 StreamTransformation，首先，会去 alreadyTransformed 中检查 StreamTransformation 是否被执行过了，如果没有被执行过，会根据 StreamTransformation 的类型调用不同的 transformXXX 方法

```java
/**
 * 通过遍历 StreamTransformations 生成一个 StreamGraph
 */
public static StreamGraph generate(StreamExecutionEnvironment env, List<StreamTransformation<?>> transformations) {
	return new StreamGraphGenerator(env).generateInternal(transformations);
}

/**
 * 生成一棵 transformation 树
 */
private StreamGraph generateInternal(List<StreamTransformation<?>> transformations) {
	for (StreamTransformation<?> transformation: transformations) {
		transform(transformation);
	}
	return streamGraph;
}

/**
 * Transform 一个 StreamTransformation
 * 这个方法会先检查 transform 参数是否已经被执行过了，如果没有的话，会根据 transform 的类型
 * 选择特定的方法来执行
 */
private Collection<Integer> transform(StreamTransformation<?> transform) {

	if (alreadyTransformed.containsKey(transform)) {
		return alreadyTransformed.get(transform);
	}

	Collection<Integer> transformedIds;
	if (transform instanceof OneInputTransformation<?, ?>) {
		transformedIds = transformOneInputTransform((OneInputTransformation<?, ?>) transform);
	} else if (transform instanceof TwoInputTransformation<?, ?, ?>) {
		transformedIds = transformTwoInputTransform((TwoInputTransformation<?, ?, ?>) transform);
	} else if (transform instanceof SourceTransformation<?>) {
		transformedIds = transformSource((SourceTransformation<?>) transform);
	} else if (transform instanceof SinkTransformation<?>) {
		transformedIds = transformSink((SinkTransformation<?>) transform);
	} else if (transform instanceof UnionTransformation<?>) {
		transformedIds = transformUnion((UnionTransformation<?>) transform);
	} else if (transform instanceof SplitTransformation<?>) {
		transformedIds = transformSplit((SplitTransformation<?>) transform);
	} else if (transform instanceof SelectTransformation<?>) {
		transformedIds = transformSelect((SelectTransformation<?>) transform);
	} else if (transform instanceof FeedbackTransformation<?>) {
		transformedIds = transformFeedback((FeedbackTransformation<?>) transform);
	} else if (transform instanceof CoFeedbackTransformation<?>) {
		transformedIds = transformCoFeedback((CoFeedbackTransformation<?>) transform);
	} else if (transform instanceof PartitionTransformation<?>) {
		transformedIds = transformPartition((PartitionTransformation<?>) transform);
	} else if (transform instanceof SideOutputTransformation<?>) {
		transformedIds = transformSideOutput((SideOutputTransformation<?>) transform);
	} else {
		throw new IllegalStateException("Unknown transformation: " + transform);
	}

	// 需要这次 check，因为迭代转换会在转换反馈边之前添加自身
	if (!alreadyTransformed.containsKey(transform)) {
		alreadyTransformed.put(transform, transformedIds);
	}

	if (transform.getBufferTimeout() >= 0) {
		streamGraph.setBufferTimeout(transform.getId(), transform.getBufferTimeout());
	}
	if (transform.getUid() != null) {
		streamGraph.setTransformationUID(transform.getId(), transform.getUid());
	}
	if (transform.getUserProvidedNodeHash() != null) {
		streamGraph.setTransformationUserHash(transform.getId(), transform.getUserProvidedNodeHash());
	}

	if (transform.getMinResources() != null && transform.getPreferredResources() != null) {
		streamGraph.setResources(transform.getId(), transform.getMinResources(), transform.getPreferredResources());
	}

	return transformedIds;
}
```

### transformSource

transformSource 调用 `streamGraph.addSource` 给 streamGraph 实例添加 source 节点  

```java
/**
 * 转换一个 SourceTransformation
 */
private <T> Collection<Integer> transformSource(SourceTransformation<T> source) {
	// 在 StreamGraph 中添加源节点
	streamGraph.addSource(source.getId(),
			slotSharingGroup,
			source.getCoLocationGroupKey(),
			source.getOperator(),
			null,
			source.getOutputType(),
			"Source: " + source.getName());

	return Collections.singleton(source.getId());
}
```

### transformSink

transformSink 方法接收 SinkTransformation，SinkTransformation 类有一个 input 字段，指代输入 StreamTransformation，transformSink 方法首先调用 `transform(sink.getInput())` 确保输入 StreamTransformation 执行完毕，然后调用 `streamGraph.addSink` 给 streamGraph 添加 sink 节点，然后在图中为 sink 节点和 inputs 中的每个节点加上边

```java
private <T> Collection<Integer> transformSink(SinkTransformation<T> sink) {

	Collection<Integer> inputIds = transform(sink.getInput());
	
	streamGraph.addSink(sink.getId(),
			slotSharingGroup,
			sink.getCoLocationGroupKey(),
			sink.getOperator(),
			sink.getInput().getOutputType(),
			null,
			"Sink: " + sink.getName());

	// 在图中为 sink 节点和 inputs 中的每个节点加上边
	for (Integer inputId: inputIds) {
		streamGraph.addEdge(inputId,
				sink.getId(),
				0
		);
	}

	return Collections.emptyList();
}
```

### transformOneInputTransform

transformOneInputTransform 和 transformSink 类似，首先确保上游的 StreamTransformation 都已经完成了转化，然后通过 `streamGraph.addOperator` 在 streamGraph 中添加节点，然后在图中为 新节点和 inputIds 中的每个节点加上边（transformTwoInputTransform 和 transformOneInputTransform 类似）

```java
private <IN, OUT> Collection<Integer> transformOneInputTransform(OneInputTransformation<IN, OUT> transform) {

	Collection<Integer> inputIds = transform(transform.getInput());

	// 递归的调用可能已经转换了这个
	if (alreadyTransformed.containsKey(transform)) {
		return alreadyTransformed.get(transform);
	}

	// 在 StreamGraph 中添加一个操作符
	streamGraph.addOperator(transform.getId(),
			slotSharingGroup,
			transform.getCoLocationGroupKey(),
			transform.getOperator(),
			transform.getInputType(),
			transform.getOutputType(),
			transform.getName());

	for (Integer inputId: inputIds) {
		streamGraph.addEdge(inputId, transform.getId(), 0);
	}

	return Collections.singleton(transform.getId());
}
```

### transformUnion

transformUnion 这个转换非常简单，我们仅仅需要转换所有输入，然后返回所有的输入 transformations 的 id，这样下游操作符能够连接所有的上游节点

```java
private <T> Collection<Integer> transformUnion(UnionTransformation<T> union) {
	List<StreamTransformation<T>> inputs = union.getInputs();
	List<Integer> resultIds = new ArrayList<>();

	for (StreamTransformation<T> input: inputs) {
		resultIds.addAll(transform(input));
	}

	return resultIds;
}
```

### transformPartition

transformPartition 用于在 streamGraph 中添加一个虚拟节点，虚拟节点中包含了 PartitionTransformation 中输入 StreamTransformation 的 id 以及 StreamPartitioner（transformSelect 和 transformSideOutput 和 transformPartition 类似，都是在 streamGraph 中添加一个虚拟节点，transformSideOutput 的虚拟节点包含 OutputTag，transformSelect 的虚拟节点包含 SelectedNames，也就是 就是 SplitStream.select() 中传递的参数）

```java
private <T> Collection<Integer> transformPartition(PartitionTransformation<T> partition) {
	StreamTransformation<T> input = partition.getInput();
	List<Integer> resultIds = new ArrayList<>();

	Collection<Integer> transformedIds = transform(input);
	for (Integer transformedId: transformedIds) {
		int virtualId = StreamTransformation.getNewNodeId();
		streamGraph.addVirtualPartitionNode(transformedId, virtualId, partition.getPartitioner());
		resultIds.add(virtualId);
	}

	return resultIds;
}
```

### transformSplit

transformSplit 用于为所有的上游节点添加 OutputSelector

```java
private <T> Collection<Integer> transformSplit(SplitTransformation<T> split) {

	StreamTransformation<T> input = split.getInput();  // 获取输入 transformation
	Collection<Integer> resultIds = transform(input);

	validateSplitTransformation(input);

	// 递归的 transform 调用可能已经转换了这个 transformation
	if (alreadyTransformed.containsKey(split)) {
		return alreadyTransformed.get(split);
	}

	for (int inputId : resultIds) {
		// 为所有输入添加 OutputSelector
		streamGraph.addOutputSelector(inputId, split.getOutputSelector());
	}

	return resultIds;
}
```

### transformFeedback

transformFeedback 用于转换 FeedbackTransformation，FeedbackTransformation 会在 streamGraph 中创建两个节点，一个迭代头节点，一个迭代尾节点。迭代尾节点用于接收反馈边传递过来的数据，然后发送给迭代头，因此迭代头和上游节点一起为下游提供数据，组成了 resultIds，同时，反馈节点和迭代尾节点之间需要加上边

```java
private <T> Collection<Integer> transformFeedback(FeedbackTransformation<T> iterate) {

	StreamTransformation<T> input = iterate.getInput();
	List<Integer> resultIds = new ArrayList<>();

	// 首先转换输入流并且存储 result IDs
	Collection<Integer> inputIds = transform(input);
	resultIds.addAll(inputIds);

	// 转换是递归的，执行到这里的时候可能已经转换过了
	if (alreadyTransformed.containsKey(iterate)) {
		return alreadyTransformed.get(iterate);
	}

	// 创建假的迭代 source/sink 对
	Tuple2<StreamNode, StreamNode> itSourceAndSink = streamGraph.createIterationSourceAndSink(
		iterate.getId(),
		getNewIterationNodeId(),
		getNewIterationNodeId(),
		iterate.getWaitTime(),
		iterate.getParallelism(),
		iterate.getMaxParallelism(),
		iterate.getMinResources(),
		iterate.getPreferredResources());

	StreamNode itSource = itSourceAndSink.f0;
	StreamNode itSink = itSourceAndSink.f1;

	// 将反馈源 id 加入到 result ids 中去，这样下游操作符会将 输入 + 反馈 一起当作输入
	// 反馈头作为消费者，因此加入 resultIds
	resultIds.add(itSource.getId());

	// 使用结果 ID 迭代到已经看到的集合时，这样我们可以转换反馈边，并在遇到迭代节点时让它们停止
	alreadyTransformed.put(iterate, resultIds);

	// 我们能够从所有的反馈边来决定 slotSharingGroup
	List<Integer> allFeedbackIds = new ArrayList<>();

	for (StreamTransformation<T> feedbackEdge : iterate.getFeedbackEdges()) {
		Collection<Integer> feedbackIds = transform(feedbackEdge);  // 生成反馈节点
		allFeedbackIds.addAll(feedbackIds);
		for (Integer feedbackId: feedbackIds) {
			// 因为反馈尾接收反馈边传来的数据，再发送给反馈头，因此反馈尾节点是作为 edge 的 targetId 的
			streamGraph.addEdge(feedbackId,
					itSink.getId(),
					0
			);
		}
	}

	// 反馈头节点和反馈尾节点的 slotSharingGroup 由所有的反馈节点共同决定
	String slotSharingGroup = determineSlotSharingGroup(null, allFeedbackIds);

	itSink.setSlotSharingGroup(slotSharingGroup);
	itSource.setSlotSharingGroup(slotSharingGroup);

	return resultIds;
}
```

## StreamGraph

### 重要属性

```java
private Map<Integer, StreamNode> streamNodes;  // 节点 map，key 是 transformation 的 id
private Set<Integer> sources;  // 图中所有的数据源头节点
private Set<Integer> sinks;  // 图中所有的下沉节点
private Map<Integer, Tuple2<Integer, List<String>>> virtualSelectNodes;  // 图中所有的 select 虚拟节点
private Map<Integer, Tuple2<Integer, OutputTag>> virtualSideOutputNodes;  // 图中所有的 side output 虚拟节点
private Map<Integer, Tuple2<Integer, StreamPartitioner<?>>> virtualPartitionNodes;  // 图中所有的 partition 虚拟节点
	
protected Map<Integer, String> vertexIDtoBrokerID;  // 存储反馈头尾节点 id 和 FeedbackTransformation id 之间的映射
protected Map<Integer, Long> vertexIDtoLoopTimeout;  // 存储反馈头尾节点 id 和 iterate 中设置的 timeout 之间的映射

private Set<Tuple2<StreamNode, StreamNode>> iterationSourceSinkPairs;  // 存储图中所有迭代头、尾组成的 pair
```

### addSource

addSource 方法在 transformSource 中被调用，用于添加源头节点。addSource 调用 addOperator 方法，然后将 vertexID 加入 sources

```java
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
```

### addSink

addSink 方法在 transformSink 中被调用，用于添加尾节点。addSink 同样调用 addOperator 方法，然后将 vertexID 加入 sinks

```java
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
```

### addOperator

addOperator 用于添加一个操作符，根据 StreamOperator 的类型，选取不同的 StreamTask 的子类，调用 addNode 方法

addOperator 在 addSource 和 addSink 中被调用，在 transformOneInputTransform 也会被调用

```java
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
}
```

### addNode

addNode 用于给 StreamGraph 添加一个 StreamNode 节点，首先校验图中是否已经存在 vertexID 这个节点，然后生成一个 StreamNode 实例，并将其加入 streamNodes 中

```java
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
```

### addVirtualSelectNode

addVirtualSelectNode 在 transformSelect 中被调用，用于将 (虚拟节点 id，<上游输入节点 id，select 选取的 selectedNames>) 加入 virtualSelectNodes

```java
public void addVirtualSelectNode(Integer originalId, Integer virtualId, List<String> selectedNames) {
	
	if (virtualSelectNodes.containsKey(virtualId)) {
		throw new IllegalStateException("Already has virtual select node with id " + virtualId);
	}
	
	virtualSelectNodes.put(virtualId,
			new Tuple2<Integer, List<String>>(originalId, selectedNames));
}
```

### addVirtualPartitionNode

addVirtualPartitionNode 在 transformPartition 中被调用，用于将 (虚拟节点 id，<上游输入节点 id，StreamPartitioner>) 加入 virtualPartitionNodes

```java
public void addVirtualPartitionNode(Integer originalId, Integer virtualId, StreamPartitioner<?> partitioner) {

	if (virtualPartitionNodes.containsKey(virtualId)) {
		throw new IllegalStateException("Already has virtual partition node with id " + virtualId);
	}

	virtualPartitionNodes.put(virtualId,
			new Tuple2<Integer, StreamPartitioner<?>>(originalId, partitioner));
}
```

### addVirtualSideOutputNode

addVirtualSideOutputNode 在 transformSideOutput 中被调用，用于将 (虚拟节点 id，<上游输入节点 id，OutputTag>) 加入 virtualSideOutputNodes

```java
public void addVirtualSideOutputNode(Integer originalId, Integer virtualId, OutputTag outputTag) {

	if (virtualSideOutputNodes.containsKey(virtualId)) {
		throw new IllegalStateException("Already has virtual output node with id " + virtualId);
	}

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
```

### addEdge && addEdgeInternal

在 StreamGraphGenerator 中，多处调用 addEdge 用来给 StreamGraph 中上下游的 StreamNode 中加上一条 StreamEdge，在 addEdge 方法内部调用了 addEdgeInternal

我们从👇可以看到，addEdgeInternal 接收的参数中，upStreamVertexID 和 downStreamVertexID 代表 edge 的上下游节点 id，type 表示边的类型，partitioner 代表边上的 StreamPartitioner，outputNames 代表边上的 selectedName 的 list，outputTag 代表边上的 OutputTag

我们可以看到，当上游节点是虚拟节点的时候，flink 会去对应的 hashMap 中获取虚拟节点的上游节点，通过递归的调用，最终上游节点会是 StreamGraph 的真实节点，然后将该节点和下游节点之间连上 StreamEdge，虚拟节点会转换为边上的属性

最后会将生成的边加入上游节点的出边集合和下游节点的入边集合

```java
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

		// 如果没有显示定义 partitioner，同时上下游操作符满足使用 forward partitioning 的条件，使用
		// forward partitioning，否则使用 rebalance
		if (partitioner == null && upstreamNode.getParallelism() == downstreamNode.getParallelism()) {
			partitioner = new ForwardPartitioner<Object>();
		} else if (partitioner == null) {
			partitioner = new RebalancePartitioner<Object>();
		}

		StreamEdge edge = new StreamEdge(upstreamNode, downstreamNode, typeNumber, outputNames, partitioner, outputTag);

		// 将边加入两端节点的入边集合和出边集合
		getStreamNode(edge.getSourceId()).addOutEdge(edge);
		getStreamNode(edge.getTargetId()).addInEdge(edge);
	}
}
```

### addOutputSelector

addOutputSelector 在 transformSplit 中被调用，用于给 StreamNode 添加 OutputSelector，可以看到，如果节点是虚拟节点，同样会去获取上游节点

```java
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
```

### createIterationSourceAndSink

createIterationSourceAndSink 在 transformFeedback 中被调用。createIterationSourceAndSink 会生成两个 StreamNode，分别指代迭代头和迭代尾，然后将两个 StreamNode 加入各种 map 中 -。-

```java
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

	StreamNode sink = this.addNode(sinkId,
		null,
		null,
		StreamIterationTail.class,  // task 类
		null,
		"IterationSink-" + loopId);
	sinks.add(sink.getId());

	iterationSourceSinkPairs.add(new Tuple2<>(source, sink));

	this.vertexIDtoBrokerID.put(source.getId(), "broker-" + loopId);
	this.vertexIDtoBrokerID.put(sink.getId(), "broker-" + loopId);
	this.vertexIDtoLoopTimeout.put(source.getId(), timeout);
	this.vertexIDtoLoopTimeout.put(sink.getId(), timeout);

	return new Tuple2<>(source, sink);
}
```

## 总结

本文主要介绍了 flink 如何根据 Stream API 编写的程序，构造出一个代表拓扑结构的 StreamGraph 的，本文的源码分析涉及到较多代码，如果有兴趣建议结合完整源码进行学习，希望对大家有所帮助