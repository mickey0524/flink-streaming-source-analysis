# flink 的 OperatorChain

从[flink 的 JobGraph](./docs/flink-job-graph.md)我们可以知道，JobGraph 会将 StreamGraph 中能够链式连接的操作符放入同一个 JobVertex 中，但是在之前的文章中，我们发现操作符处理流元素基本遵循一个套路 —— 通过 processElement 方法接受到来的流元素，经过 userFunction 处理后，通过 output emit 给下游，那么，在操作链中 processElement 方法是在何处被调用的，output 方法如何将流元素 emit 给链中或链外的下游操作符呢，今天这篇文章我们就来一探究竟

## OperatorChain 的重要属性

```java
// 所有的操作符，最后每一个 StreamNode 都会包含一个 StreamOperator
// 因为不含 StreamOperator 的 transformation 都变成 StreamEdge 的属性了
private final StreamOperator<?>[] allOperators;

// output，用于 emit 元素给链外的操作符
private final RecordWriterOutput<?>[] streamOutputs;

// headOperator 的 collector output
private final WatermarkGaugeExposingOutput<StreamRecord<OUT>> chainEntryPoint;

// 链条头部操作符 StreamOperator
private final OP headOperator;
```

## OperatorChain 的构造函数

OperatorChain 方法会在 `StreamTask.java` 中被调用，传入的 containingTask 就是包含这条操作符链的 StreamTask，recordWriters 是一个 RecordWriter 的 list，recordWriters 的size 等于操作符链中连接到链外的边的数量

flink 会在 JobGraph 生成的时候调用 setVertexConfig 将操作符的配置写入 StreamConfig，在 OperatorChain 中，我们从中取出 headOperator 以及链操作符配置（由链中所有操作符的 StreamConfig 组成）

OperatorChain 会为 chain 的每一条出边创建一个 RecordWriterOutput 实例，RecordWriterOutput 中包裹着 RecordWriter

最后 OperatorChain 会调用 createOutputCollector 方法，创建一个负责将流元素写入 chain 的 output collector，然后将 output collector 通过 headOperator.setup 方法写入 chain 的头部操作符

```java
public OperatorChain(
		StreamTask<OUT, OP> containingTask,
		List<RecordWriter<SerializationDelegate<StreamRecord<OUT>>>> recordWriters) {

	final ClassLoader userCodeClassloader = containingTask.getUserCodeClassLoader();
	final StreamConfig configuration = containingTask.getConfiguration();

	// 获取头部操作符
	headOperator = configuration.getStreamOperator(userCodeClassloader);

	// 我们读取链式配置，按输出名称安排 record writer 的顺序
	Map<Integer, StreamConfig> chainedConfigs = configuration.getTransitiveChainedTaskConfigsWithSelf(userCodeClassloader);

	// 我们获取 JobVertex（操作链） 的所有出边，然后生成 stream output
	List<StreamEdge> outEdgesInOrder = configuration.getOutEdgesInOrder(userCodeClassloader);
	Map<StreamEdge, RecordWriterOutput<?>> streamOutputMap = new HashMap<>(outEdgesInOrder.size());
	this.streamOutputs = new RecordWriterOutput<?>[outEdgesInOrder.size()];

	// 从这里开始，我们需要确保 output writers 在失败时再次关闭。
	boolean success = false;
	try {
		for (int i = 0; i < outEdgesInOrder.size(); i++) {
			StreamEdge outEdge = outEdgesInOrder.get(i);
			// 对 chain 的每一个出边，创建一个 streamOutput
			RecordWriterOutput<?> streamOutput = createStreamOutput(
				recordWriters.get(i),
				outEdge,
				chainedConfigs.get(outEdge.getSourceId()),
				containingTask.getEnvironment());

			this.streamOutputs[i] = streamOutput;
			streamOutputMap.put(outEdge, streamOutput);
		}

		// 我们创建了操作链，同时掌握了通向链的 collector
		List<StreamOperator<?>> allOps = new ArrayList<>(chainedConfigs.size());
		this.chainEntryPoint = createOutputCollector(
			containingTask,
			configuration,
			chainedConfigs,
			userCodeClassloader,
			streamOutputMap,
			allOps);

		if (headOperator != null) {
			WatermarkGaugeExposingOutput<StreamRecord<OUT>> output = getChainEntryPoint();
			headOperator.setup(containingTask, configuration, output);

			headOperator.getMetricGroup().gauge(MetricNames.IO_CURRENT_OUTPUT_WATERMARK, output.getWatermarkGauge());
		}

		// 添加头部操作符到 chain 的尾巴
		allOps.add(headOperator);

		this.allOperators = allOps.toArray(new StreamOperator<?>[allOps.size()]);

		success = true;
	}
	finally {
		// 确保失败的时候，关闭所有的 output
		// output 都是从 Environment getWriter 获取的
		// 需要关闭
		if (!success) {
			for (RecordWriterOutput<?> output : this.streamOutputs) {
				if (output != null) {
					output.close();
				}
			}
		}
	}
}
```

### createOutputCollector

createOutputCollector 用于创建 AbstractStreamOperator 的 output，在 AbstractStreamOperator 中 output 用于将操作符处理完毕的流元素 emit 给下游，拿 StreamMap 举个例子

```java
public class StreamMap<IN, OUT>
		extends AbstractUdfStreamOperator<OUT, MapFunction<IN, OUT>>
		implements OneInputStreamOperator<IN, OUT> {

	private static final long serialVersionUID = 1L;

	public StreamMap(MapFunction<IN, OUT> mapper) {
		super(mapper);
		chainingStrategy = ChainingStrategy.ALWAYS;  // map 会在当前 Thread 内执行
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		output.collect(element.replace(userFunction.map(element.getValue())));
	}
}
```

接下来，我们来看看 createOutputCollector 的源码

allOutputs 用于存储所有的 output，包括 emit 元素到链中的下游操作符 output 和链外的操作符的 output，首先，从操作符 StreamConfig 中获取 nonChainedOutputs，里面存储了 chain 到链外的边，在构造函数中，我们已经为这些边创建好了 RecordWriterOutput，然后，从操作符 StreamConfig 中获取 ChainedOutputs，里面存储了 chain 内所有的边，调用 createChainedOperator 生成操作符用的 output

随后，获取节点的 selectors，selectors 是由 OutputSelector 组成的 list，OutputSelector 是通过 split 方法设置的，用于配合 select 方法传入的 selectedName 选择性的输出，如果 selectors 为 null 或 selectors 为空，说明元素可以从所有的边流下去，选择 BroadcastingOutputCollector，否则需要选择更加复杂的 DirectedOutput

```java
/**
 * 创建 OutputCollector
 * @param containingTask 执行的 task
 * @param operatorConfig StreamNode 的配置
 * @param chainedConfigs 链中所有节点的配置
 * @param userCodeClassloader 类加载器
 * @param streamOutputs JobVertex 所有的输出
 * @param allOperators 链中所有的操作符，传递过来的时候是个空的 ArrayList
 */
private <T> WatermarkGaugeExposingOutput<StreamRecord<T>> createOutputCollector(
		StreamTask<?, ?> containingTask,
		StreamConfig operatorConfig,
		Map<Integer, StreamConfig> chainedConfigs,
		ClassLoader userCodeClassloader,
		Map<StreamEdge, RecordWriterOutput<?>> streamOutputs,
		List<StreamOperator<?>> allOperators) {
	List<Tuple2<WatermarkGaugeExposingOutput<StreamRecord<T>>, StreamEdge>> allOutputs = new ArrayList<>(4);

	// 为 JobVertex 的输出创建 collector
	for (StreamEdge outputEdge : operatorConfig.getNonChainedOutputs(userCodeClassloader)) {
		@SuppressWarnings("unchecked")
		RecordWriterOutput<T> output = (RecordWriterOutput<T>) streamOutputs.get(outputEdge);

		allOutputs.add(new Tuple2<>(output, outputEdge));
	}

	// 为链内部的输出创建 collector
	for (StreamEdge outputEdge : operatorConfig.getChainedOutputs(userCodeClassloader)) {
		// 链内部，edge 在 StreamGraph 中的目标节点
		int outputId = outputEdge.getTargetId();
		// 获取目标节点的配置
		StreamConfig chainedOpConfig = chainedConfigs.get(outputId);

		WatermarkGaugeExposingOutput<StreamRecord<T>> output = createChainedOperator(
			containingTask,
			chainedOpConfig,
			chainedConfigs,
			userCodeClassloader,
			streamOutputs,
			allOperators,
			outputEdge.getOutputTag());
		allOutputs.add(new Tuple2<>(output, outputEdge));
	}

	// 如果有多个输出，或者输出是定向的，我们需要将它们包装为一个输出
	// split 操作的参数 OutputSelector
	List<OutputSelector<T>> selectors = operatorConfig.getOutputSelectors(userCodeClassloader);

	if (selectors == null || selectors.isEmpty()) {
		// 只有一个输出
		if (allOutputs.size() == 1) {
			return allOutputs.get(0).f0;
		}
		else {
			// 有多个输出，需要注意的是，存在特殊情况没有输出
			@SuppressWarnings({"unchecked", "rawtypes"})
			Output<StreamRecord<T>>[] asArray = new Output[allOutputs.size()];
			for (int i = 0; i < allOutputs.size(); i++) {
				asArray[i] = allOutputs.get(i).f0;
			}

			return new BroadcastingOutputCollector<>(asArray, this);
	}
	else {
		// 存在选择器，需要更复杂的路由
		return new DirectedOutput<>(selectors, allOutputs);
	}
}
```

#### BroadcastingOutputCollector

当节点没有设置 OutputSelector，选用 BroadcastingOutputCollector，从源码中可以清晰的看到，BroadcastingOutputCollector 的方法对应调用 outputs 中每一个 output 相应的方法，output 用于 emit 流元素到链中或链外操作符

```java
static class BroadcastingOutputCollector<T> implements WatermarkGaugeExposingOutput<StreamRecord<T>> {

	protected final Output<StreamRecord<T>>[] outputs;  // 所有的输出，当元素来的时候，需要传递给所有的 output

	private final Random random = new XORShiftRandom();

	private final StreamStatusProvider streamStatusProvider;

	private final WatermarkGauge watermarkGauge = new WatermarkGauge();

	public BroadcastingOutputCollector(
			Output<StreamRecord<T>>[] outputs,
			StreamStatusProvider streamStatusProvider) {
		this.outputs = outputs;
		this.streamStatusProvider = streamStatusProvider;
	}

	@Override
	public void emitWatermark(Watermark mark) {
		watermarkGauge.setCurrentWatermark(mark.getTimestamp());
		if (streamStatusProvider.getStreamStatus().isActive()) {
			for (Output<StreamRecord<T>> output : outputs) {
				output.emitWatermark(mark);
			}
		}
	}

	@Override
	// 延迟 marker 随机发送给一个 output
	public void emitLatencyMarker(LatencyMarker latencyMarker) {
		if (outputs.length <= 0) {
			// ignore
		} else if (outputs.length == 1) {
			outputs[0].emitLatencyMarker(latencyMarker);
		} else {
			// randomly select an output
			outputs[random.nextInt(outputs.length)].emitLatencyMarker(latencyMarker);
		}
	}

	@Override
	public Gauge<Long> getWatermarkGauge() {
		return watermarkGauge;
	}

	@Override
	public void collect(StreamRecord<T> record) {
		for (Output<StreamRecord<T>> output : outputs) {
			output.collect(record);
		}
	}

	@Override
	public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {
		for (Output<StreamRecord<T>> output : outputs) {
			output.collect(outputTag, record);
		}
	}

	@Override
	public void close() {
		for (Output<StreamRecord<T>> output : outputs) {
			output.close();
		}
	}
}
```

#### DirectedOutput

当节点设置了 OutputSelector，选用 DirectedOutput，outputSelectors 中存放节点的 OutputSelector，selectAllOutputs 存放节点的边上没有 selectedName 的出边对应的 output，这说明所有的流元素都能够流过这条边，outputMap 是一个 map，将 selectedName 作为 k，v 为边上设置了 selectedName 的出边对应的 output，说明只有流元素经过 OutputSelector 计算的结果与 k 相等，才能从 v 中的 output emit

在 collect 方法被调用的时候，首先会调用 OutputSelector 计算流元素得到结果，只有结果对应的 outputs 和 selectAllOutputs 能够 emit 到来的流元素

```java
public class DirectedOutput<OUT> implements OperatorChain.WatermarkGaugeExposingOutput<StreamRecord<OUT>> {

	protected final OutputSelector<OUT>[] outputSelectors;  // 输出选择器，split 方法的参数

	protected final Output<StreamRecord<OUT>>[] selectAllOutputs;

	protected final HashMap<String, Output<StreamRecord<OUT>>[]> outputMap;

	protected final Output<StreamRecord<OUT>>[] allOutputs;  // 所有的输出

	private final Random random = new XORShiftRandom();

	protected final WatermarkGauge watermarkGauge = new WatermarkGauge();

	@SuppressWarnings({"unchecked", "rawtypes"})
	public DirectedOutput(
			List<OutputSelector<OUT>> outputSelectors,
			List<? extends Tuple2<? extends Output<StreamRecord<OUT>>, StreamEdge>> outputs) {
		this.outputSelectors = outputSelectors.toArray(new OutputSelector[outputSelectors.size()]);

		this.allOutputs = new Output[outputs.size()];
		for (int i = 0; i < outputs.size(); i++) {
			allOutputs[i] = outputs.get(i).f0;
		}

		// StreamEdge 上没有 selectedName，将 output 写入 selectAllOutputs
		HashSet<Output<StreamRecord<OUT>>> selectAllOutputs = new HashSet<Output<StreamRecord<OUT>>>();
		// StreamEdge 有 selectedName，根据 selectedName 进行区分
		HashMap<String, ArrayList<Output<StreamRecord<OUT>>>> outputMap = new HashMap<String, ArrayList<Output<StreamRecord<OUT>>>>();

		for (Tuple2<? extends Output<StreamRecord<OUT>>, StreamEdge> outputPair : outputs) {
			final Output<StreamRecord<OUT>> output = outputPair.f0;  // collector output
			final StreamEdge edge = outputPair.f1;  // 输出边

			List<String> selectedNames = edge.getSelectedNames();  // select 方法的参数

			if (selectedNames.isEmpty()) {
				selectAllOutputs.add(output);
			}
			else {
				for (String selectedName : selectedNames) {
					if (!outputMap.containsKey(selectedName)) {
						outputMap.put(selectedName, new ArrayList<Output<StreamRecord<OUT>>>());
						outputMap.get(selectedName).add(output);
					}
					else {
						if (!outputMap.get(selectedName).contains(output)) {
							outputMap.get(selectedName).add(output);
						}
					}
				}
			}
		}

		this.selectAllOutputs = selectAllOutputs.toArray(new Output[selectAllOutputs.size()]);

		this.outputMap = new HashMap<>();
		for (Map.Entry<String, ArrayList<Output<StreamRecord<OUT>>>> entry : outputMap.entrySet()) {
			Output<StreamRecord<OUT>>[] arr = entry.getValue().toArray(new Output[entry.getValue().size()]);
			this.outputMap.put(entry.getKey(), arr);
		}
	}

	@Override
	public void emitWatermark(Watermark mark) {
		watermarkGauge.setCurrentWatermark(mark.getTimestamp());
		for (Output<StreamRecord<OUT>> out : allOutputs) {
			out.emitWatermark(mark);
		}
	}

	@Override
	public void emitLatencyMarker(LatencyMarker latencyMarker) {
		// randomly select an output
		// 随机选择一个输出
		allOutputs[random.nextInt(allOutputs.length)].emitLatencyMarker(latencyMarker);
	}

	protected Set<Output<StreamRecord<OUT>>> selectOutputs(StreamRecord<OUT> record)  {
		// 全输出的 output 没有限制，直接 addAll
		Set<Output<StreamRecord<OUT>>> selectedOutputs = new HashSet<>(selectAllOutputs.length);
		Collections.addAll(selectedOutputs, selectAllOutputs);

		for (OutputSelector<OUT> outputSelector : outputSelectors) {
			// 根据 split 中的 outputSelector 求出 record 的 outputNames
			Iterable<String> outputNames = outputSelector.select(record.getValue());

			// 然后根据 outputNames 与 select 中的进行比对，匹配上了才输出
			for (String outputName : outputNames) {
				Output<StreamRecord<OUT>>[] outputList = outputMap.get(outputName);
				if (outputList != null) {
					Collections.addAll(selectedOutputs, outputList);
				}
			}
		}

		return selectedOutputs;
	}

	@Override
	public void collect(StreamRecord<OUT> record) {
		Set<Output<StreamRecord<OUT>>> selectedOutputs = selectOutputs(record);

		for (Output<StreamRecord<OUT>> out : selectedOutputs) {
			out.collect(record);
		}
	}

	@Override
	/**
	 * split，select 和 side output 是互斥的
	 */
	public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {
		throw new UnsupportedOperationException("Cannot use split/select with side outputs.");
	}

	@Override
	public void close() {
		for (Output<StreamRecord<OUT>> out : allOutputs) {
			out.close();
		}
	}

	@Override
	public Gauge<Long> getWatermarkGauge() {
		return watermarkGauge;
	}
}
```

### createChainedOperator

createChainedOperator 用于得到链中操作符对应的 output，createChainedOperator 会递归调用 createOutputCollector 方法（解释见代码中的注释）得到 chainedOperatorOutput，然后从 StreamConfig 中获取 operator，调用 setup 方法将 chainedOperatorOutput 写入操作符，然后创建一个 ChainingOutput 实例返回

```java
/**
 * 创建链式操作符
 * @param containingTask 执行的 task
 * @param operatorConfig StreamNode 的配置
 * @param chainedConfigs 链中所有节点的配置
 * @param userCodeClassloader 类加载器
 * @param streamOutputs JobVertex 所有的输出
 * @param allOperators 链中所有的操作符，传递过来的时候是个空的 ArrayList
 * @param outputTag StreamNode 入边的 outputTag
 */
private <IN, OUT> WatermarkGaugeExposingOutput<StreamRecord<IN>> createChainedOperator(
		StreamTask<?, ?> containingTask,
		StreamConfig operatorConfig,
		Map<Integer, StreamConfig> chainedConfigs,
		ClassLoader userCodeClassloader,
		Map<StreamEdge, RecordWriterOutput<?>> streamOutputs,
		List<StreamOperator<?>> allOperators,
		OutputTag<IN> outputTag) {
	// 创建操作符首先写入的 output，可能会递归创建更多的操作符
	// 会一直递归调用，直到 createOutputCollector 调用的时候，当前操作符没有链式下游节点
	// 也就是说会先处理链式的 end 节点，然后生成一个操作符连接链式结尾和 JobVertex 外的节点
	// 然后根据这个操作符生成一个 output，沿着链式往上走，这样数据才能流下去
	WatermarkGaugeExposingOutput<StreamRecord<OUT>> chainedOperatorOutput = createOutputCollector(
		containingTask,
		operatorConfig,
		chainedConfigs,
		userCodeClassloader,
		streamOutputs,
		allOperators);

	// 现在创建一个操作符，并且给操作符一个 output collector，操作符将输出写入 collector
	OneInputStreamOperator<IN, OUT> chainedOperator = operatorConfig.getStreamOperator(userCodeClassloader);

	// 调用操作符的 setup 方法，启动链式操作符
	// 这里传进去了 chainedOperatorOutput
	// chainedOperator 会将 chainedOperatorOutput 作为 output
	// 在 chainedOperator 的 processElement 方法内
	// 会调用 output.collect()
	chainedOperator.setup(containingTask, operatorConfig, chainedOperatorOutput);

	// 将操作符加入 list，越前面的操作符在 list 中越靠后
	allOperators.add(chainedOperator);

	WatermarkGaugeExposingOutput<StreamRecord<IN>> currentOperatorOutput;

	currentOperatorOutput = new ChainingOutput<>(chainedOperator, this, outputTag);

	return currentOperatorOutput;
}
```

#### ChainingOutput

ChainingOutput 就是调用操作符 processElement 的实例，用于将流元素写入操作符，ChainingOutput 是链式操作的关键，大家可以想一下，headOperator 的 processElement 方法处理完流元素之后，会调用 `output.collect`，如果存在 output
指向链外，调用上文讲述过的 BroadcastingOutputCollector/DirectedOutput，这两个 output 是对外的，collect 方法会调用 recordWriterOutput，链内的 output 就是 ChainingOutput，collect 方法用于调用下游操作符的 processElement 方法，从而让流元素在 chain 中流动，这样大大减少了流元素传输的消耗

```java
static class ChainingOutput<T> implements WatermarkGaugeExposingOutput<StreamRecord<T>> {

	protected final OneInputStreamOperator<T, ?> operator;
	protected final Counter numRecordsIn;
	protected final WatermarkGauge watermarkGauge = new WatermarkGauge();

	protected final StreamStatusProvider streamStatusProvider;

	@Nullable
	protected final OutputTag<T> outputTag;

	public ChainingOutput(
			OneInputStreamOperator<T, ?> operator,
			StreamStatusProvider streamStatusProvider,
			@Nullable OutputTag<T> outputTag) {
		this.operator = operator;

		{
			Counter tmpNumRecordsIn;
			try {
				OperatorIOMetricGroup ioMetricGroup = ((OperatorMetricGroup) operator.getMetricGroup()).getIOMetricGroup();
				tmpNumRecordsIn = ioMetricGroup.getNumRecordsInCounter();
			} catch (Exception e) {
				LOG.warn("An exception occurred during the metrics setup.", e);
				tmpNumRecordsIn = new SimpleCounter();
			}
			numRecordsIn = tmpNumRecordsIn;
		}

		this.streamStatusProvider = streamStatusProvider;
		this.outputTag = outputTag;
	}

	@Override
	public void collect(StreamRecord<T> record) {
		if (this.outputTag != null) {
			// 这个方法只被用于 emit 主输出
			// we are only responsible for emitting to the main input
			return;
		}

		pushToOperator(record);
	}

	@Override
	public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {
		if (this.outputTag == null || !this.outputTag.equals(outputTag)) {
			// we are only responsible for emitting to the side-output specified by our
			// OutputTag.
			// 这个方法只被用于输出 outputTag 和 ChainingOutput 的 outputTag 相同的侧边输出
			return;
		}

		pushToOperator(record);
	}

	protected <X> void pushToOperator(StreamRecord<X> record) {
		try {
			// we know that the given outputTag matches our OutputTag so the record
			// must be of the type that our operator expects.
			@SuppressWarnings("unchecked")
			StreamRecord<T> castRecord = (StreamRecord<T>) record;

			numRecordsIn.inc();
			operator.setKeyContextElement1(castRecord);
			operator.processElement(castRecord); // 这个方法会调用操作符的 processElement 方法
		}
		catch (Exception e) {
			throw new ExceptionInChainedOperatorException(e);
		}
	}

	@Override
	public void emitWatermark(Watermark mark) {
		try {
			watermarkGauge.setCurrentWatermark(mark.getTimestamp());
			if (streamStatusProvider.getStreamStatus().isActive()) {
				// 当流状态是 ACTIVE 的时候，执行操作符的 processWatermark 方法
				operator.processWatermark(mark);
			}
		}
		catch (Exception e) {
			throw new ExceptionInChainedOperatorException(e);
		}
	}

	@Override
	public void emitLatencyMarker(LatencyMarker latencyMarker) {
		try {
			operator.processLatencyMarker(latencyMarker);
		}
		catch (Exception e) {
			throw new ExceptionInChainedOperatorException(e);
		}
	}

	@Override
	public void close() {
		try {
			operator.close();
		}
		catch (Exception e) {
			throw new ExceptionInChainedOperatorException(e);
		}
	}

	@Override
	public Gauge<Long> getWatermarkGauge() {
		return watermarkGauge;
	}
}
```

## 总结

今天讲解了 flink 的 OperatorChain，这是 flink 中非常重要且精髓的一个类，希望大家能细嚼慢咽，这个类在之后的 checkpoint 中还会遇到～