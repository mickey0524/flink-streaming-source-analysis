# flink KeyedStream 的转换

上一篇文章介绍了 flink 中 DataStream 的转换算子，本篇文章我们来介绍一下 KeyedStream 的转换，KeyedStream 是通过 DataStream 执行 keyBy 操作转换而来

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

## 🚧 Under Construction...



