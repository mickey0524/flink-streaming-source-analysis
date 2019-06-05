# flink 的窗口 —— 窗口流

窗口流代表了一种靠 WindowAssigner 分配窗口的数据流，可以通过 reduce、max、sum、aggregate 等 API 来生成 WindowOperator／EvictingWindowOperator 操作符，完成窗口操作

窗口流分为 AllWindowedStream 和 WindowedStream，AllWindowedStream 由 DataStream 执行 windowAll 得到，WindowedStream 由 KeyedStream 执行 window 得到。AllWindowedStream 和 WindowedStream 基本相同，因为 AllWindowedStream 的构造函数中会对输入 DataStream 执行 keyBy 操作，传入如下的 KeySelector，为所有的元素设置一个 0 的 key，mock 一个 KeyedStream

```java
public class NullByteKeySelector<T> implements KeySelector<T, Byte> {
	@Override
	public Byte getKey(T value) throws Exception {
		return 0;
	}
}
```

我们就用 WindowedStream 来讲解吧，之前[flink 的窗口 —— 窗口函数](./docs/flink-window-function.md)中的窗口函数都是配套 WindowedStream 使用的

## WindowedStream 的属性

```java
// 输入流，WindowedStream 的各种 API 都需要调用 input 的 transform 方法
// 生成 OneInputTransformation 加入 transformation 树
private final KeyedStream<T, Byte> input;  

// 窗口分配器
private final WindowAssigner<? super T, W> windowAssigner;  
	
// 触发器
private Trigger<? super T, ? super W> trigger;  
	
// 驱逐者
private Evictor<? super T, ? super W> evictor;  
	
// 用于定义的允许的延迟
private long allowedLateness = 0L;  

// 针对延迟数据的侧边输出，如果没有设置 lateDataOutputTag，延迟数据会被丢弃
private OutputTag<T> lateDataOutputTag;
```

## WindowedStream 的 reduce 方法

reduce 方法需要用户至少传入一个 ReduceFunction，和 KeyedStream 中的 reduce 类似，对窗口中的所有元素进行聚合

* 不传 WindowFunction
 
	不传 WindowFunction 的时候，flink 会默认传入一个 PassThroughWindowFunction 实例
	
	```java
	public SingleOutputStreamOperator<T> reduce(ReduceFunction<T> function) {

		function = input.getExecutionEnvironment().clean(function);

		String callLocation = Utils.getCallLocationName();
		String udfName = "AllWindowedStream." + callLocation;

		return reduce(function, new PassThroughWindowFunction<W, T>());
	}
	```

* 传 WindowFunction

	reduce 方法接收一个 ReduceFunction 以及一个 WindowFunction，然后，根据 WindowedStream 是否设置了 evictor，创建不同的 StateDescriptor（原因见下方代码）。当设置了 evictor 的时候，会创建 EvictingWindowOperator，反之，创建 WindowOperator，需要注意的是，函数的最后调用了 forceNonParallel 方法，因此并行度为 1

	```java
	public <R> SingleOutputStreamOperator<R> reduce(
			ReduceFunction<T> reduceFunction,
			WindowFunction<T, R, W> function,
			TypeInformation<R> resultType) {

		function = input.getExecutionEnvironment().clean(function);
		reduceFunction = input.getExecutionEnvironment().clean(reduceFunction);

		String callLocation = Utils.getCallLocationName();
		String udfName = "WindowedStream." + callLocation;

		String opName;
		KeySelector<T, K> keySel = input.getKeySelector();

		OneInputStreamOperator<T, R> operator;

		if (evictor != null) {
			// 当有驱逐者的时候，EvictingWindowOperator 中
			// evictingWindowState 需要保存所有的元素，在 emitContent
			// 中执行 evictBefore 和 evictAfter
			// 因此这里选择了 ListStateDescriptor
			// 同时，我们需要自己来 reduce，所以包裹了 ReduceApplyAllWindowFunction
			TypeSerializer<StreamRecord<T>> streamRecordSerializer =
				(TypeSerializer<StreamRecord<T>>) new StreamElementSerializer(input.getType().createSerializer(getExecutionEnvironment().getConfig()));

			ListStateDescriptor<StreamRecord<T>> stateDesc =
				new ListStateDescriptor<>("window-contents", streamRecordSerializer);

			opName = "TriggerWindow(" + windowAssigner + ", " + stateDesc + ", " + trigger + ", " + evictor + ", " + udfName + ")";

			operator =
				new EvictingWindowOperator<>(windowAssigner,
					windowAssigner.getWindowSerializer(getExecutionEnvironment().getConfig()),
					keySel,
					input.getKeyType().createSerializer(getExecutionEnvironment().getConfig()),
					stateDesc,
					new InternalIterableWindowFunction<>(new ReduceApplyWindowFunction<>(reduceFunction, function)),
					trigger,
					evictor,
					allowedLateness,
					lateDataOutputTag);

		} else {
			// 状态描述符，因为没有驱逐者，所以不需要保留原始值
			// 直接在状态描述符这里创建一个 ReducingStateDescriptor
			// 到时候 WindowOperator 里 windowState.add(element.getValue()) 的时候
			// windowState 直接就进行了 reduce 操作
			// 因此这里的 InternalWindowFunction 是 InternalSingleValueWindowFunction
			// 因为经过 reduce 已经是一个数值了
			ReducingStateDescriptor<T> stateDesc = new ReducingStateDescriptor<>("window-contents",
				reduceFunction,
				input.getType().createSerializer(getExecutionEnvironment().getConfig()));

			opName = "TriggerWindow(" + windowAssigner + ", " + stateDesc + ", " + trigger + ", " + udfName + ")";

			operator =
				new WindowOperator<>(windowAssigner,
					windowAssigner.getWindowSerializer(getExecutionEnvironment().getConfig()),
					keySel,
					input.getKeyType().createSerializer(getExecutionEnvironment().getConfig()),
					stateDesc,
					new InternalSingleValueWindowFunction<>(function),
					trigger,
					allowedLateness,
					lateDataOutputTag);
		}

		return input.transform(opName, resultType, operator);
	}
	```

* 传 ProcessWindowFunction

	传 ProcessWindowFunction 和传 WindowFunction 的情况差不多，只是选取的窗口函数不同（不细说了）

	```java
	public <R> SingleOutputStreamOperator<R> reduce(ReduceFunction<T> reduceFunction, ProcessAllWindowFunction<T, R, W> function, TypeInformation<R> resultType) {

		function = input.getExecutionEnvironment().clean(function);
		reduceFunction = input.getExecutionEnvironment().clean(reduceFunction);

		String callLocation = Utils.getCallLocationName();
		String udfName = "WindowedStream." + callLocation;

		String opName;
		KeySelector<T, K> keySel = input.getKeySelector();

		OneInputStreamOperator<T, R> operator;

		// 这里 InternalWindowFunction 和 StateDescriptor 的取舍和上面 reduce 函数中写的一样
		if (evictor != null) {
			@SuppressWarnings({"unchecked", "rawtypes"})
			TypeSerializer<StreamRecord<T>> streamRecordSerializer =
				(TypeSerializer<StreamRecord<T>>) new StreamElementSerializer(input.getType().createSerializer(getExecutionEnvironment().getConfig()));

			ListStateDescriptor<StreamRecord<T>> stateDesc =
				new ListStateDescriptor<>("window-contents", streamRecordSerializer);

			opName = "TriggerWindow(" + windowAssigner + ", " + stateDesc + ", " + trigger + ", " + evictor + ", " + udfName + ")";

			operator =
				new EvictingWindowOperator<>(windowAssigner,
					windowAssigner.getWindowSerializer(getExecutionEnvironment().getConfig()),
					keySel,
					input.getKeyType().createSerializer(getExecutionEnvironment().getConfig()),
					stateDesc,
					new InternalIterableProcessWindowFunction<>(new ReduceApplyProcessWindowFunction<>(reduceFunction, function)),
					trigger,
					evictor,
					allowedLateness,
					lateDataOutputTag);

		} else {
			ReducingStateDescriptor<T> stateDesc = new ReducingStateDescriptor<>("window-contents",
				reduceFunction,
				input.getType().createSerializer(getExecutionEnvironment().getConfig()));

			opName = "TriggerWindow(" + windowAssigner + ", " + stateDesc + ", " + trigger + ", " + udfName + ")";

			operator =
				new WindowOperator<>(windowAssigner,
					windowAssigner.getWindowSerializer(getExecutionEnvironment().getConfig()),
					keySel,
					input.getKeyType().createSerializer(getExecutionEnvironment().getConfig()),
					stateDesc,
					new InternalSingleValueProcessWindowFunction<>(function),
					trigger,
					allowedLateness,
					lateDataOutputTag);
		}

		return input.transform(opName, resultType, operator);
	}
	```

## WindowedStream 的 sum/max/min/maxBy/minBy 方法

WindowedStream 中 sum/max/min/maxBy/minBy 这些方法和 KeyedStream 中 sum/max/min/maxBy/minBy 相同，也是调用 reduce 方法，因为 AggregationFunction 实现了 ReduceFunction 接口

```java
public SingleOutputStreamOperator<T> sum(int positionToSum) {
	return aggregate(new SumAggregator<>(positionToSum, input.getType(), input.getExecutionConfig()));
}

private SingleOutputStreamOperator<T> aggregate(AggregationFunction<T> aggregator) {
	return reduce(aggregator);
}
```

## WindowedStream 的 aggregate 方法

aggregate 方法和 reduce 方法类似，只是把 reduce 中的 ReduceFunction 换成了 AggregateFunction，这里给出一个接收 WindowFunction 的 aggregate 方法源码吧

```java
public <ACC, V, R> SingleOutputStreamOperator<R> aggregate(
		AggregateFunction<T, ACC, V> aggregateFunction,
		WindowFunction<V, R, W> windowFunction,
		TypeInformation<ACC> accumulatorType,
		TypeInformation<R> resultType) {

	windowFunction = input.getExecutionEnvironment().clean(windowFunction);
	aggregateFunction = input.getExecutionEnvironment().clean(aggregateFunction);

	final String callLocation = Utils.getCallLocationName();
	final String udfName = "WindowedStream." + callLocation;

	final String opName;
	final KeySelector<T, K> keySel = input.getKeySelector();

	OneInputStreamOperator<T, R> operator;

	// StateDescriptor 的选择和 InternalWindowFunction 的选择和
	// 上面 reduce 的原因相同
	if (evictor != null) {
		TypeSerializer<StreamRecord<T>> streamRecordSerializer =
				(TypeSerializer<StreamRecord<T>>) new StreamElementSerializer(
						input.getType().createSerializer(getExecutionEnvironment().getConfig()));

		ListStateDescriptor<StreamRecord<T>> stateDesc =
				new ListStateDescriptor<>("window-contents", streamRecordSerializer);

		opName = "TriggerWindow(" + windowAssigner + ", " + stateDesc + ", " + trigger + ", " + evictor + ", " + udfName + ")";

		operator =
				new EvictingWindowOperator<>(windowAssigner,
						windowAssigner.getWindowSerializer(getExecutionEnvironment().getConfig()),
						keySel,
						input.getKeyType().createSerializer(getExecutionEnvironment().getConfig()),
						stateDesc,
						new InternalIterableWindowFunction<>(
								new AggregateApplyWindowFunction<>(aggregateFunction, windowFunction)),
						trigger,
						evictor,
						allowedLateness,
						lateDataOutputTag);

	} else {
		AggregatingStateDescriptor<T, ACC, V> stateDesc = new AggregatingStateDescriptor<>(
				"window-contents",
				aggregateFunction,
				accumulatorType.createSerializer(getExecutionEnvironment().getConfig()));

		opName = "TriggerWindow(" + windowAssigner + ", " + stateDesc + ", " + trigger + ", " + udfName + ")";

		operator = new WindowOperator<>(
						windowAssigner,
						windowAssigner.getWindowSerializer(getExecutionEnvironment().getConfig()),
						keySel,
						input.getKeyType().createSerializer(getExecutionEnvironment().getConfig()),
						stateDesc,
						new InternalSingleValueWindowFunction<>(windowFunction),
						trigger,
						allowedLateness,
						lateDataOutputTag);
	}

	return input.transform(opName, resultType, operator);
}
```

## WindowedStream 的 apply 方法

如果不想对窗口进行任何聚合操作，可以调用 apply 方法，直接传入 WindowFunction 或 ProcessWindowFunction，apply 方法会将其包装成 InternalWindowFunction

从下面的代码可以看到，无论是否设置了 evictor，都是选用 ListStateDescriptor，直接将集合传递给 WindowFunction 或 ProcessWindowFunction

```java
private <R> SingleOutputStreamOperator<R> apply(InternalWindowFunction<Iterable<T>, R, Byte, W> function, TypeInformation<R> resultType, String callLocation) {

	String udfName = "AllWindowedStream." + callLocation;

	String opName;
	KeySelector<T, K> keySel = input.getKeySelector();

	WindowOperator<K, T, Iterable<T>, R, W> operator;

	// 因为没有增量聚合操作，所以 StateDescriptor 都是 ListStateDescriptor
	if (evictor != null) {
		TypeSerializer<StreamRecord<T>> streamRecordSerializer =
				(TypeSerializer<StreamRecord<T>>) new StreamElementSerializer(input.getType().createSerializer(getExecutionEnvironment().getConfig()));

		ListStateDescriptor<StreamRecord<T>> stateDesc =
				new ListStateDescriptor<>("window-contents", streamRecordSerializer);

		opName = "TriggerWindow(" + windowAssigner + ", " + stateDesc + ", " + trigger + ", " + evictor + ", " + udfName + ")";

		operator =
			new EvictingWindowOperator<>(windowAssigner,
				windowAssigner.getWindowSerializer(getExecutionEnvironment().getConfig()),
				keySel,
				input.getKeyType().createSerializer(getExecutionEnvironment().getConfig()),
				stateDesc,
				function,
				trigger,
				evictor,
				allowedLateness,
				lateDataOutputTag);

	} else {
		ListStateDescriptor<T> stateDesc = new ListStateDescriptor<>("window-contents",
				input.getType().createSerializer(getExecutionEnvironment().getConfig()));

		opName = "TriggerWindow(" + windowAssigner + ", " + stateDesc + ", " + trigger + ", " + udfName + ")";

		operator =
			new WindowOperator<>(windowAssigner,
				windowAssigner.getWindowSerializer(getExecutionEnvironment().getConfig()),
				keySel,
				input.getKeyType().createSerializer(getExecutionEnvironment().getConfig()),
				stateDesc,
				function,
				trigger,
				allowedLateness,
				lateDataOutputTag);
	}

	return input.transform(opName, resultType, operator);
}
```

## 窗口操作小栗子

```java
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

String host = "127.0.0.1";
int port = 9000;

DataStream<String> dataStream = env.socketTextStream(host, port);
dataStream.map(new MapFunction<String, Tuple2<String, Integer>>() {
    @Override
    public Tuple2<String, Integer> map(String value) throws Exception {
        return Tuple2.of(value, 1);
    }
}).keyBy(0)
        .window(TumblingProcessingTimeWindows.of(Time.of(5, TimeUnit.SECONDS)))
        .sum(1)
        .printToErr();
```

## 总结

以上四篇文章，我们从四个方面介绍了 flink 中的窗口操作，窗口是 flink 的一大特色，希望大家可以好好理解～

