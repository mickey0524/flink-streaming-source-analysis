# flink KeyedStream 的转换

上一篇文章介绍了 flink 中 DataStream 的转换算子，本篇文章我们来介绍一下 KeyedStream 的转换，KeyedStream 是通过 DataStream 执行 keyBy 操作转换而来，KeyedStream 常用的转换包括 reduce 和 aggregate（窗口函数相关的 intervalJoin 将在之后的文章中讲解）

## reduce

reduce 对数据进行聚合操作，结合当前元素和上一次 reduce 返回的值进行聚合操作，然后返回一个新的值，flink 会 emit 所有 reduce 的中间输出

使用 reduce 操作拼接字符串

```java
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
String[] strings = new String[]{"i", "love", "flink"};

DataStream<String> dataStream = env.fromElements(strings);
dataStream.keyBy(new KeySelector<String, Byte>() {
    @Override
    public Byte getKey(String value) throws Exception {
        return 0;
    }
}).reduce(new ReduceFunction<String>() {
    @Override
    public String reduce(String value1, String value2) throws Exception {
        return value1 + " " + value2;
    }
}).printToErr();
```

接下来看看源码是如何实现的

flink 中有一个 `StreamGroupedReduce.java` 的文件，位于 `org.apache.flink.streaming.api.operators. StreamGroupedReduce.java`，StreamGroupedReduce 是一个 StreamOperator，用于在流元素到来的时候调用定义的 ReduceFunction 进行聚合，并 emit 处理后的流元素

```java
public class StreamGroupedReduce<IN> extends AbstractUdfStreamOperator<IN, ReduceFunction<IN>>
		implements OneInputStreamOperator<IN, IN> {

	private static final long serialVersionUID = 1L;

	private static final String STATE_NAME = "_op_state";

	private transient ValueState<IN> values;  // 存储当前 KeyedStream 处理的当前状态

	private TypeSerializer<IN> serializer;

	public StreamGroupedReduce(ReduceFunction<IN> reducer, TypeSerializer<IN> serializer) {
		super(reducer);
		this.serializer = serializer;
	}

	@Override
	public void open() throws Exception {
		super.open();
		ValueStateDescriptor<IN> stateId = new ValueStateDescriptor<>(STATE_NAME, serializer);
		values = getPartitionedState(stateId);
	}

	@Override
	// reduce 的中间输出也会被 collect
	public void processElement(StreamRecord<IN> element) throws Exception {
		IN value = element.getValue();
		IN currentValue = values.value();

		if (currentValue != null) {
			IN reduced = userFunction.reduce(currentValue, value);
			values.update(reduced);
			output.collect(element.replace(reduced));
		} else {
			values.update(value);
			output.collect(element.replace(value));
		}
	}
}
```

reduce 操作需要保存当前已聚合的状态，因此在 open 的时候，创建了一个 ValueStateDescriptor 实例，进而得到了一个 ValueState 实例，reduce 会将已聚合的值存入 ValueState，在新的元素到来的时候，从 ValueState 中取值，执行 ReduceFunction，并将处理后的元素再次存入 ValueState

## fold

fold 和 reduce 操作基本相同，同样是对数据进行聚合操作，fold 可以设置一个初始值，reduce 的输出类型和输入类型必须相同，fold 的输出类型和设置的初始值类型相同

```java
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
String[] strings = new String[]{"1", "2", "3"};

DataStream<String> dataStream = env.fromElements(strings);
dataStream.keyBy(new KeySelector<String, Byte>() {
    @Override
    public Byte getKey(String value) throws Exception {
        return 0;
    }
}).fold(1, new FoldFunction<String, Integer>() {
    @Override
    public Integer fold(Integer accumulator, String value) throws Exception {
        return accumulator + Integer.valueOf(value);
    }

}).printToErr();
```

这里就不分析 fold 的源码了，和 reduce 类似，有兴趣的同学可以去 `org.apache.flink.streaming.api.operators.StreamGroupedFold.java` 查看

## sum

sum 函数用于对整形和浮点型的数据进行累加，如果待聚合的类型不为 Integer、Long、Short、Byte、Double 和 Float 中的一种，flink 会抛出异常

```java
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
Integer[] integers = new Integer[]{1, 2, 3};

DataStream<Integer> dataStream = env.fromElements(integers);

dataStream.keyBy(new KeySelector<Integer, Byte>() {

    @Override
    public Byte getKey(Integer value) throws Exception {
        return 0;
    }
}).sum(0).printToErr();
```

下面我们来看看源码

```java
protected SingleOutputStreamOperator<T> aggregate(AggregationFunction<T> aggregate) {
	StreamGroupedReduce<T> operator = new StreamGroupedReduce<T>(
			clean(aggregate), getType().createSerializer(getExecutionConfig()));
	return transform("Keyed Aggregation", getType(), operator);
}
```

从代码中我们可以看到，所有的 aggregate 操作都会被转换为一个 reduce 操作，也就是说 aggregate 操作需要提供一个 ReduceFunction

```java
public abstract class AggregationFunction<T> implements ReduceFunction<T> {
	private static final long serialVersionUID = 1L;

	public enum AggregationType {
		SUM, MIN, MAX, MINBY, MAXBY,
	}
}
```

果然，AggregationFunction 抽象类实现了 ReduceFunction 的结果，AggregationType 则对应了 5 种 aggregate 操作

```java
public class SumAggregator<T> extends AggregationFunction<T> {

	private static final long serialVersionUID = 1L;

	private final FieldAccessor<T, Object> fieldAccessor;  // field 访问器，会根据 type 选择合适的访问器
	private final SumFunction adder;  // sum 工具，将流中的数据 sum 起来
	private final TypeSerializer<T> serializer;

	@Override
	@SuppressWarnings("unchecked")
	public T reduce(T value1, T value2) throws Exception {
		if (isTuple) {
			Tuple result = ((Tuple) value1).copy();
			return fieldAccessor.set((T) result, adder.add(fieldAccessor.get(value1), fieldAccessor.get(value2)));
		} else {
			T result = serializer.copy(value1);
			return fieldAccessor.set(result, adder.add(fieldAccessor.get(value1), fieldAccessor.get(value2)));
		}
	}
}
```

SumAggregator 继承了 AggregationFunction，实现了 reduce 方法，调用 `adder.add()` 方法进行累加

## max/maxBy/min/minBy

max 和 maxBy 用于获取分组中最大的数值，min 和 minBy 用于获取分组中最小的数值，参与比较的元素是需要实现 Comparable 接口的

```java
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
Integer[] integers = new Integer[]{1, 2, 3};

DataStream<Integer> dataStream = env.fromElements(integers);

dataStream.keyBy(new KeySelector<Integer, Byte>() {

    @Override
    public Byte getKey(Integer value) throws Exception {
        return 0;
    }
}).max(0).printToErr();
```

下面来看看源码，和 sum 操作类似，max 操作也需要提供一个继承 AggregationFunction 的类，max、min、maxBy 和 minBy 操作都是使用相同的类，这里详细介绍一下

* Comparator 是一个 flink 实现的比较器，用于获取两个流元素字段的比较结果
* byAggregate 指代当前操作是否是 maxBy 或 minBy 操作
* first 指代 maxBy 和 minBy 操作中，当两个流元素参与比较的字段相等的时候，选取哪一个流元素，当 first 为 true 的时候，选取之前到来的流元素，反之，选择之后到来的流元素，这里注意，只有 maxBy 和 minBy 操作能够传递 first 这个字段，当 max 和 min 操作中两个流元素参与比较的字段相等的时候，flink 会将后到来的流元素中参与比较的字段写入之前到来的流元素中，也就是说，max 和 min 操作永远保留之前到来的元素
* fieldAccessor 用于提取流元素参与比较的字段

从代码中，我们可以清晰的看到，首先，提取两个流元素中参与比较的字段，然后调用 comparator 比较器的 isExtremal 方法得到比较的结果

当操作是 maxBy 或 minBy 的时候，`c == 0` 的时候，表明比较结果相等，根据 first 字段决定返回 value1 还是 value2，`c == 1` 返回 value1，`c == 0` 返回 value2

当操作是 max 或 min 的时候，`c == 0` 的时候，会将 o2 写入 value1，无论比较结果如何，永远返回 value1

```java
public class ComparableAggregator<T> extends AggregationFunction<T> {

	private static final long serialVersionUID = 1L;

	private Comparator comparator;
	private boolean byAggregate;
	private boolean first;
	private final FieldAccessor<T, Object> fieldAccessor;
	
	@SuppressWarnings("unchecked")
	@Override
	public T reduce(T value1, T value2) throws Exception {
		Comparable<Object> o1 = (Comparable<Object>) fieldAccessor.get(value1);
		Object o2 = fieldAccessor.get(value2);

		int c = comparator.isExtremal(o1, o2);  // 取出 value1 和 value2 的键进行大小比较

		if (byAggregate) {
			// MAXBY/MINBY 直接返回符合条件的键
			// if they are the same we choose based on whether we want to first or last
			// element with the min/max.
			// 当 value1 和 value2 相等的时候
			// 根据 first 属性来选择 value1 还是 value2
			if (c == 0) {
				return first ? value1 : value2;
			}

			return c == 1 ? value1 : value2;

		} else {
			// MAX/MIN 将符合条件的 key 写入 value1，MAX/MIN 返回的永远是 value1，主要注意的是，当 o2 等于 o1 的时候
			// 会将 o2 写入 value1
			if (c == 0) {
				value1 = fieldAccessor.set(value1, o2);
			}
			return value1;
		}

	}
}
```

我们分 api 来介绍 Comparator

* max 操作的 Comparator，只有 `o1.compareTo(o2) > 0` 返回 1，当 o2 大于等于 o1 的时候，都是返回 0 的

	```java
	private static class MaxComparator extends Comparator {
	
		private static final long serialVersionUID = 1L;
	
		@Override
		public <R> int isExtremal(Comparable<R> o1, R o2) {
			return o1.compareTo(o2) > 0 ? 1 : 0;
		}
	
	}
	```

* min 操作的 Comparator，只有 `o1.compareTo(o2) < 0` 返回 1，当 o2 小于等于 o1 的时候，都是返回 0 的

	```java
	private static class MinComparator extends Comparator {
	
		private static final long serialVersionUID = 1L;
	
		@Override
		public <R> int isExtremal(Comparable<R> o1, R o2) {
			return o1.compareTo(o2) < 0 ? 1 : 0;
		}
	
	}
	```

* maxBy 操作的 Comparator，直接返回 `o1.compareTo(o2)` 的结果

	```java
	private static class MaxByComparator extends Comparator {

		private static final long serialVersionUID = 1L;

		@Override
		public <R> int isExtremal(Comparable<R> o1, R o2) {
			int c = o1.compareTo(o2);
			if (c > 0) {
				return 1;
			}
			if (c == 0) {
				return 0;
			} else {
				return -1;
			}
		}

	}
	```

* minBy 操作的 Comparator，返回 `o1.compareTo(o2)` 的相反数

	```java
	private static class MinByComparator extends Comparator {

		private static final long serialVersionUID = 1L;

		@Override
		public <R> int isExtremal(Comparable<R> o1, R o2) {
			int c = o1.compareTo(o2);
			if (c < 0) {
				return 1;
			}
			if (c == 0) {
				return 0;
			} else {
				return -1;
			}
		}

	}
	```

## process

上一篇文章中也有对 process 的介绍，DataStream 的 process 只能用来进行偏侧输出，而 KeyedStream 的 process 还能够操作定时器，原因是定时器需要 keyedStateBackend 来进行 checkpoint（后面的文章中会介绍）

```java
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
Integer[] integers = new Integer[]{1, 2, 3};

DataStream<Integer> dataStream = env.addSource(new SourceFunction<Integer>() {

    private volatile boolean isRunning = true;

    @Override
    public void run(SourceContext<Integer> ctx) throws Exception {
        int idx = 0;
        while (isRunning) {
            if (idx < integers.length) {
                ctx.collect(integers[idx++]);
            }
        }
    }

    @Override
    public void cancel() {
        this.isRunning = false;
    }
});

dataStream.keyBy(new KeySelector<Integer, Byte>() {

    @Override
    public Byte getKey(Integer value) throws Exception {
        return 0;
    }
}).process(new KeyedProcessFunction<Byte, Integer, Integer>() {
    @Override
    public void processElement(Integer value, Context ctx, Collector<Integer> out) throws Exception {
        out.collect(value);

        ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 5000);
    }

    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Integer> out) throws Exception {
        out.collect(-1);
    }
}).printToErr();
```

接下来看看源码（截取重要部分）是如何实现的

flink 中有一个 `KeyedProcessOperator.java` 的文件，位于 `org.apache.flink.streaming.api.operators.KeyedProcessOperator.java`，KeyedProcessOperator 是一个 StreamOperator，用于在流元素到来的时候调用定义的 KeyedProcessFunction，并 emit 处理后的流元素，KeyedProcessOperator 使用 TimestampedCollector 来保证输出的流元素的 timestamp 全部等于输入的流元素，使用 InternalTimerService 来访问定时器，使用 ContextImpl 提供 KeyedProcessOperator 偏侧输出的能力，使用 OnTimerContextImpl 提供 KeyedProcessOperator 在定时器触发的时候再次访问定时器以及偏侧输出的能力

```java
public class KeyedProcessOperator<K, IN, OUT>
		extends AbstractUdfStreamOperator<OUT, KeyedProcessFunction<K, IN, OUT>>
		implements OneInputStreamOperator<IN, OUT>, Triggerable<K, VoidNamespace> {

	private static final long serialVersionUID = 1L;

	private transient TimestampedCollector<OUT> collector;  // 保证 process 出来的元素 ts 相同

	private transient ContextImpl context;  // 实现 KeyedProcessFunction.Context

	private transient OnTimerContextImpl onTimerContext;  // 实现 KeyedProcessFunction.OnTimerContext

	public KeyedProcessOperator(KeyedProcessFunction<K, IN, OUT> function) {
		super(function);

		chainingStrategy = ChainingStrategy.ALWAYS;
	}

	@Override
	public void open() throws Exception {
		super.open();
		collector = new TimestampedCollector<>(output);

		// 这里没有用 ProcessTimeService 而是使用了 InternalTimerService
		InternalTimerService<VoidNamespace> internalTimerService =
				getInternalTimerService("user-timers", VoidNamespaceSerializer.INSTANCE, this);

		TimerService timerService = new SimpleTimerService(internalTimerService);

		context = new ContextImpl(userFunction, timerService);
		onTimerContext = new OnTimerContextImpl(userFunction, timerService);
	}

	@Override
	// open 中生成 internalTimerService 的时候，传入 this 作为 trigger，这是 event 时间定时器的回调函数
	// 会在 InternalTimerServiceImpl 中的 advanceWatermark 函数内被调用
	public void onEventTime(InternalTimer<K, VoidNamespace> timer) throws Exception {
		collector.setAbsoluteTimestamp(timer.getTimestamp());
		invokeUserFunction(TimeDomain.EVENT_TIME, timer);
	}

	@Override
	// open 中生成 internalTimerService 的时候，传入 this 作为 trigger，这是 process 时间定时器的回调函数
	// 会在 InternalTimerServiceImpl 中的 onProcessingTime 函数内被调用
	public void onProcessingTime(InternalTimer<K, VoidNamespace> timer) throws Exception {
		collector.eraseTimestamp();
		invokeUserFunction(TimeDomain.PROCESSING_TIME, timer);
	}
	
	// 处理到来的流元素
	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		collector.setTimestamp(element);
		context.element = element;  // 将当前处理的 StreamRecord 写入 context 中
		// 调用 KeyedProcessFunction 处理流元素
		userFunction.processElement(element.getValue(), context, collector);
		context.element = null;
	}

	// 定时器触发的时候调用 invokeUserFunction 函数
	private void invokeUserFunction(
			TimeDomain timeDomain,
			InternalTimer<K, VoidNamespace> timer) throws Exception {
		onTimerContext.timeDomain = timeDomain;  // 将定时器的 timeDomain 写入 onTimerContext
		onTimerContext.timer = timer;  // 将定时器写入 onTimerContext
		// 执行定时器回调函数
		userFunction.onTimer(timer.getTimestamp(), onTimerContext, collector);
		onTimerContext.timeDomain = null;
		onTimerContext.timer = null;
	}

	private class ContextImpl extends KeyedProcessFunction<K, IN, OUT>.Context {
		
		// 这是与 ProcessOperator 中最大的不同，这里传入了一个 TimerService，ProcessOperator 中 TimerService 是一个 dummy
		private final TimerService timerService;

		private StreamRecord<IN> element;

		ContextImpl(KeyedProcessFunction<K, IN, OUT> function, TimerService timerService) {
			function.super();
			this.timerService = checkNotNull(timerService);
		}

		// 返回当前处理的元素的 ts
		@Override
		public Long timestamp() {
			checkState(element != null);

			if (element.hasTimestamp()) {
				return element.getTimestamp();
			} else {
				return null;
			}
		}

		// 这里就和 ProcessOperator 中不一样，这里返回的是包裹 internalTimerService 的 SimpleTimerService
		// 因此能够操作定时器
		@Override
		public TimerService timerService() {
			return timerService;
		}
		
		// 偏侧输出
		@Override
		public <X> void output(OutputTag<X> outputTag, X value) {
			if (outputTag == null) {
				throw new IllegalArgumentException("OutputTag must not be null.");
			}

			output.collect(outputTag, new StreamRecord<>(value, element.getTimestamp()));
		}

		@Override
		@SuppressWarnings("unchecked")
		public K getCurrentKey() {
			return (K) KeyedProcessOperator.this.getCurrentKey();
		}
	}

	private class OnTimerContextImpl extends KeyedProcessFunction<K, IN, OUT>.OnTimerContext {

		private final TimerService timerService;

		private TimeDomain timeDomain;

		private InternalTimer<K, VoidNamespace> timer;

		OnTimerContextImpl(KeyedProcessFunction<K, IN, OUT> function, TimerService timerService) {
			function.super();
			this.timerService = checkNotNull(timerService);
		}

		// 返回触发的定时器的时间
		@Override
		public Long timestamp() {
			checkState(timer != null);
			return timer.getTimestamp();
		}

		@Override
		public TimerService timerService() {
			return timerService;
		}

		@Override
		public <X> void output(OutputTag<X> outputTag, X value) {
			if (outputTag == null) {
				throw new IllegalArgumentException("OutputTag must not be null.");
			}

			output.collect(outputTag, new StreamRecord<>(value, timer.getTimestamp()));
		}

		@Override
		public TimeDomain timeDomain() {
			checkState(timeDomain != null);
			return timeDomain;
		}

		// 返回 timer 的 key
		@Override
		public K getCurrentKey() {
			return timer.getKey();
		}
	}
}
```

## 总结

本文主要介绍了 flink KeyedStream 中的常用转换方式，大家可以结合上一篇文章来学习，每一个算子都有配套的小 demo 和源码分析，具体在项目中该如何将数据流转换成我们想要的格式，还需要根据实际情况对待

