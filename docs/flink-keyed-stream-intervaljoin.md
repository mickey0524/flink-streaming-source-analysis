# flink KeyedStream 的 intervalJoin

这篇文章我们来介绍一下 KeyedStream 中的 intervalJoin 操作，intervalJoin 和字面意思一样，指的是时间段的 join

## KeyedStream 的 intervalJoin

intervalJoin 接收一个 KeyedStream，生成一个 IntervalJoin 实例

```java
public <T1> IntervalJoin<T, T1, KEY> intervalJoin(KeyedStream<T1, KEY> otherStream) {
	return new IntervalJoin<>(this, otherStream);
}
```

## KeyedStream 的 IntervalJoin

IntervalJoin 中包裹着两个 KeyedStream，IntervalJoin 提供一个 between 方法，用来定义 join 操作的上下界，between 方法会返回一个 IntervalJoined 实例，需要注意的是，between 方法仅仅支持在 EventTime 模式下使用

```java
public static class IntervalJoin<T1, T2, KEY> {

	private final KeyedStream<T1, KEY> streamOne;  // 第一个 KeyedStream
	private final KeyedStream<T2, KEY> streamTwo;  // 第二个 KeyedStream

	IntervalJoin(
		KeyedStream<T1, KEY> streamOne,
		KeyedStream<T2, KEY> streamTwo
	) {
		this.streamOne = checkNotNull(streamOne);
		this.streamTwo = checkNotNull(streamTwo);
	}

	/**
	 * 定义 join 操作工作的时间段
	 */
	@PublicEvolving
	public IntervalJoined<T1, T2, KEY> between(Time lowerBound, Time upperBound) {

		TimeCharacteristic timeCharacteristic =
			streamOne.getExecutionEnvironment().getStreamTimeCharacteristic();

		// 仅仅支持在 EventTime 下使用
		if (timeCharacteristic != TimeCharacteristic.EventTime) {
			throw new UnsupportedTimeCharacteristicException("Time-bounded stream joins are only supported in event time");
		}

		checkNotNull(lowerBound, "A lower bound needs to be provided for a time-bounded join");
		checkNotNull(upperBound, "An upper bound needs to be provided for a time-bounded join");

		return new IntervalJoined<>(
			streamOne,
			streamTwo,
			lowerBound.toMilliseconds(),
			upperBound.toMilliseconds(),
			true,
			true
		);
	}
}
```

## KeyedStream 的 IntervalJoined

IntervalJoined 是一个包含两个流的容器，同时也包含 keySelector 和时间边界，process 方法接受一个 ProcessJoinFunction，生成一个 IntervalJoinOperator，然后对 left 和 right 执行 connect 操作，最后将包含 IntervalJoinOperator 的 TwoInputTransformation 加入到 transform 树中

```java
public static class IntervalJoined<IN1, IN2, KEY> {

	private final KeyedStream<IN1, KEY> left;
	private final KeyedStream<IN2, KEY> right;

	private final long lowerBound;
	private final long upperBound;

	private final KeySelector<IN1, KEY> keySelector1;
	private final KeySelector<IN2, KEY> keySelector2;

	private boolean lowerBoundInclusive;  // 时间段是否包含下界
	private boolean upperBoundInclusive;  // 时间段是否包含下界

	public IntervalJoined(
			KeyedStream<IN1, KEY> left,
			KeyedStream<IN2, KEY> right,
			long lowerBound,
			long upperBound,
			boolean lowerBoundInclusive,
			boolean upperBoundInclusive) {

		this.left = checkNotNull(left);
		this.right = checkNotNull(right);

		this.lowerBound = lowerBound;
		this.upperBound = upperBound;

		this.lowerBoundInclusive = lowerBoundInclusive;
		this.upperBoundInclusive = upperBoundInclusive;

		this.keySelector1 = left.getKeySelector();
		this.keySelector2 = right.getKeySelector();
	}
	
	public <OUT> SingleOutputStreamOperator<OUT> process(
			ProcessJoinFunction<IN1, IN2, OUT> processJoinFunction,
			TypeInformation<OUT> outputType) {
		Preconditions.checkNotNull(processJoinFunction);
		Preconditions.checkNotNull(outputType);

		final ProcessJoinFunction<IN1, IN2, OUT> cleanedUdf = left.getExecutionEnvironment().clean(processJoinFunction);

		final IntervalJoinOperator<KEY, IN1, IN2, OUT> operator =
			new IntervalJoinOperator<>(
				lowerBound,
				upperBound,
				lowerBoundInclusive,
				upperBoundInclusive,
				left.getType().createSerializer(left.getExecutionConfig()),
				right.getType().createSerializer(right.getExecutionConfig()),
				cleanedUdf
			);

		return left
			.connect(right)  // 返回一个 ConnectedStream
			.keyBy(keySelector1, keySelector2)  // 将 ConnectedStream 的两个 input 设为 KeyedStream
			.transform("Interval Join", outputType, operator);
	}
}
```

## ProcessJoinFunction

一个处理两个连接元素并生成输出元素的函数

```java
public abstract class ProcessJoinFunction<IN1, IN2, OUT> extends AbstractRichFunction {
	/**
	 * 每个连接对都会调用这个方法
	 * 这个方法通过给定的 Collector 输出零个或者多个元素
	 * 通过可以通过 Context 来访问连接元素的时间戳
	 */
	public abstract void processElement(IN1 left, IN2 right, Context ctx, Collector<OUT> out) throws Exception;

	/**
	 * 在 processElement 方法执行的时候，Context 可以被使用
	 * Context 能够访问连接对左端元素的时间戳，右端元素的时间戳
	 * 以及连接对的时间戳（join pair 中更大的 ts），此外，Context 允许偏侧输出
	 */
	public abstract class Context {

		/**
		 * 返回 join 对中左边元素的 ts
		 */
		public abstract long getLeftTimestamp();

		/**
		 * 返回 join 对中右边元素的 ts
		 */
		public abstract long getRightTimestamp();

		/**
		 * 返回 join 对的 ts
		 */
		public abstract long getTimestamp();

		/**
		 * 提供侧边输出的接口
		 */
		public abstract <X> void output(OutputTag<X> outputTag, X value);
	}
}
```

## IntervalJoinOperator

IntervalJoinOperator 是用于执行 intervalJoin 的操作符，当两个元素被连接成一个 pair 的时候，它们就被传递给用户定义的 ProcessJoinFunction

### 重要属性

```java
private final long lowerBound;  // 下界
private final long upperBound;  // 上界

private transient MapState<Long, List<BufferEntry<T1>>> leftBuffer;  // 缓存第一个流的元素
private transient MapState<Long, List<BufferEntry<T2>>> rightBuffer;  // 缓存第二个流的元素

private transient TimestampedCollector<OUT> collector;  // 统一时间的输出
private transient ContextImpl context;  // ProcessJoinFunction 的 Context

private transient InternalTimerService<String> internalTimerService;  // 内部时间服务
```

### processElement

processElement1 用于处理到达 ConnectedStream（IntervalJoined.process 中执行 connect 获得）中 input1 的流元素，同理，processElement2 用于处理到达 input2 的流元素

processElement1 和 processElement2 都会调用 processElement 方法，processElement 中先将到来的元素写入 buffer 中，buffer 是一个 map，k 是元素的 ts，v 是 BufferEntry 组成的 list，BufferEntry 内部包裹了流元素，然后遍历另一个流的 buffer，如果 buffer 中的时间戳满足
recordTs + lowerBound <= bufferTs <= recordTs + upperBound，则说明匹配上了，将该到来的元素和时间戳对应的 list 一一配对并交给 ProcessJoinFunction，此外，为了避免 buffer 数据无限量增长，注册定时器，定时清除无用元素

```java
/** 
 * 处理左边流的元素，无论何时，当 StreamRecord 到达左边的流，元素会被加入 leftBuffer
 * 会去右边的流中找何时的 join 对，如果时间合适，pair 会被传递给 ProcessJoinFunction
 */
public void processElement1(StreamRecord<T1> record) throws Exception {
	processElement(record, leftBuffer, rightBuffer, lowerBound, upperBound, true);
}

/**
 * 处理右边流的元素，无论何时，当 StreamRecord 到达右边的流，元素会被加入 leftBuffer
 * 会去左边的流中找何时的 join 对，如果时间合适，pair 会被传递给 ProcessJoinFunction
 */
public void processElement2(StreamRecord<T2> record) throws Exception {
	processElement(record, rightBuffer, leftBuffer, -upperBound, -lowerBound, false);
}

private <THIS, OTHER> void processElement(
	final StreamRecord<THIS> record,
	final MapState<Long, List<IntervalJoinOperator.BufferEntry<THIS>>> ourBuffer,
	final MapState<Long, List<IntervalJoinOperator.BufferEntry<OTHER>>> otherBuffer,
	final long relativeLowerBound,
	final long relativeUpperBound,
	final boolean isLeft) throws Exception {

	final THIS ourValue = record.getValue();
	final long ourTimestamp = record.getTimestamp();

	if (ourTimestamp == Long.MIN_VALUE) {
		throw new FlinkException("Long.MIN_VALUE timestamp: Elements used in " +
				"interval stream joins need to have timestamps meaningful timestamps.");
	}

	if (isLate(ourTimestamp)) {
		return;
	}

	addToBuffer(ourBuffer, ourValue, ourTimestamp);

	/**
	 * 遍历另外一个 buffer，查询是否有合适的元素可以形成 join 对
	 */
	for (Map.Entry<Long, List<BufferEntry<OTHER>>> bucket: otherBuffer.entries()) {
		final long timestamp = bucket.getKey();

		// 不满足条件直接丢弃
		if (timestamp < ourTimestamp + relativeLowerBound ||
				timestamp > ourTimestamp + relativeUpperBound) {
			continue;
		}

		for (BufferEntry<OTHER> entry: bucket.getValue()) {
			if (isLeft) {
				// 到来的元素是左边流的
				collect((T1) ourValue, (T2) entry.element, ourTimestamp, timestamp);
			} else {
				collect((T1) entry.element, (T2) ourValue, timestamp, ourTimestamp);
			}
		}
	}

	// 注册定时器，避免 buffer 无限制的增大，定时清除
	long cleanupTime = (relativeUpperBound > 0L) ? ourTimestamp + relativeUpperBound : ourTimestamp;
	if (isLeft) {
		internalTimerService.registerEventTimeTimer(CLEANUP_NAMESPACE_LEFT, cleanupTime);
	} else {
		internalTimerService.registerEventTimeTimer(CLEANUP_NAMESPACE_RIGHT, cleanupTime);
	}
}
```

### collect

collect 在 processElement 中被调用，用于将配对成功的 pair 传递给 processJoinFunction

```java
// 将 join pair 交给 ProcessJoinFunction 执行
private void collect(T1 left, T2 right, long leftTimestamp, long rightTimestamp) throws Exception {
	final long resultTimestamp = Math.max(leftTimestamp, rightTimestamp);

	collector.setAbsoluteTimestamp(resultTimestamp);
	context.updateTimestamps(leftTimestamp, rightTimestamp, resultTimestamp);

	userFunction.processElement(left, right, context, collector);
}
```

### onEventTime

onEventTime 用于在定时器触发的时候，清除 buffer 中的无用元素，避免 buffer 体积无限量增长

```java
// 定时清除无用元素
public void onEventTime(InternalTimer<K, String> timer) throws Exception {

	long timerTimestamp = timer.getTimestamp();
	String namespace = timer.getNamespace();

	switch (namespace) {
		case CLEANUP_NAMESPACE_LEFT: {
			long timestamp = (upperBound <= 0L) ? timerTimestamp : timerTimestamp - upperBound;
			logger.trace("Removing from left buffer @ {}", timestamp);
			leftBuffer.remove(timestamp);
			break;
		}
		case CLEANUP_NAMESPACE_RIGHT: {
			long timestamp = (lowerBound <= 0L) ? timerTimestamp + lowerBound : timerTimestamp;
			logger.trace("Removing from right buffer @ {}", timestamp);
			rightBuffer.remove(timestamp);
			break;
		}
		default:
			throw new RuntimeException("Invalid namespace " + namespace);
	}
}
```

## 总结

今天我们讲解了 KeyedStream 中的 intervalJoin 操作，希望对大家有所帮助