# 从源码角度简要分析一下 flink 流式处理小栗子

在上一票文章中，我们用两个小栗子给大家介绍了 flink 的流式处理，今天我们从源码的角度来简要分析一下 flink 内部的实现

ps：今天的文章不涉及 flink 内部的 StreamGraph（之后会有专门的文章）

首先，我们需要知道几个 flink 中非常重要的类 —— StreamExecutionEnvironment、DataStream、StreamTransformation 和 StreamOperator

## StreamExecutionEnvironment

StreamExecutionEnvironment 是一个抽象类，指代流程序执行的上下文，流执行环境能够设置流作业的并行度、设置是否开启链式操作符、设置容错机制、创建数据流源、以及最重要的 —— 存储当前流任务中的 StreamTransformation，用于绘制 StreamGraph

StreamExecutionEnvironment 有两个实现类，LocalStreamEnvironment 和 RemoteStreamEnvironment，
LocalStreamEnvironment 使得流程序在当前的 JVM 中执行，而 RemoteStreamEnvironment 将导致流程序在远程机器中执行

```java
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
DataStream<String> source = env.socketTextStream(host, port);
```

👆的代码创建一个 StreamExecutionEnvironment，然后从 socket 接收数据创建源头 DataStream

## DataStream

DataStream 指代一个数据流，一个 DataStream 可以通过 map/filter 等操作转换为其他数据流，这里简要介绍一下（之后会有专门的文章）

* map 操作

	DataStream → DataStream：取出一个元素，产生一个元素
	
	比如，使用 map 将数据元素乘以 2
	
	```java
	Integer[] nums = new Integer[]{1, 2, 3, 4};
	DataStream<Integer> dataStream = env.fromElements(nums);
	
	dataStream.map(new MapFunction<Integer, Integer>() {
        @Override
        public Integer map(Integer value) throws Exception {
            return value * 2;
        }
    });
	```
	
* filter 操作

	DataStream → DataStream：每个函数都去执行一个布尔函数，并保留使得布尔函数返回为 true 的元素 
	
	比如，保留数据流中的偶数
	
	```java
	Integer[] nums = new Integer[]{1, 2, 3, 4};
	DataStream<Integer> dataStream = env.fromElements(nums);
	
	dataStream.filter(new FilterFunction<Integer>() {
        @Override
        public boolean filter(Integer value) throws Exception {
            return value % 2 == 0;
        }
    });
	```

## StreamTransformation

一个 StreamTransformation 代表了创建一个 DataStream 的操作，每一个 DataStream 都有一个 StreamTransformation 与之对应，例如执行 map 或 filter 得到的 DataStream，与之对应的 StreamTransformation 就是 OneInputTransformation

## StreamOperator

StreamOperator 是 flink 中处理流元素的类，例如 OneInputStreamOperator 中 processElement 方法用于处理流元素，processWatermark 方法用于处理 watermark，processLatencyMarker 用于处理延迟标记

## 源码分析

为了方面大家对照着看，我们将上一篇文章中的栗子粘贴到这里

```java
package com.my.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SocketTextStreamWordCount {
    public static void main(String[] args) throws Exception {
        final c env = StreamExecutionEnvironment.getExecutionEnvironment();

        String host = "127.0.0.1";
        int port = 9000;
        // 这里从 socket 中创建一个 source
        DataStream<String> source = env.socketTextStream(host, port);
        source.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String value) throws Exception {
                try {
                    int mapValue = Integer.valueOf(value);
                    return mapValue * 10;
                } catch (Exception e) {
                    return -1;
                }
            }
        }).printToErr();

        env.execute("simple flink test");
    }
}
```

### 生成 DataStream

栗子中通过 `env.socketTextStream(host, port)` 生成一个 DataStream，底层实现的话，首先会调用 StreamExecutionEnvironment 的 `addSource(new SocketTextStreamFunction(hostname, port, delimiter, maxRetry)`，SocketTextStreamFunction 是产生流元素的函数，也就是 SourceFunction，下面我们看看 addSource 函数的核心部分

```java
public <OUT> DataStreamSource<OUT> addSource(SourceFunction<OUT> function, String sourceName, TypeInformation<OUT> typeInfo) {
	StreamSource<OUT, ?> sourceOperator = new StreamSource<>(function);

	return new DataStreamSource<>(this, typeInfo, sourceOperator, isParallel, sourceName);
}

public DataStreamSource(StreamExecutionEnvironment environment,
		TypeInformation<T> outTypeInfo, StreamSource<T, ?> operator,
		boolean isParallel, String sourceName) {
	super(environment, new SourceTransformation<>(sourceName, operator, outTypeInfo, environment.getParallelism()));

	this.isParallel = isParallel;
	if (!isParallel) {
		setParallelism(1);
	}
}
```

StreamSource 是一个 StreamOperator，其中有一个 run 方法，内部调用了 `userFunction.run(ctx)` 来将流元素写入 flink，本例子中 `userFunction` 就是从 socket 中获取元素的 SocketTextStreamFunction

DataStreamSource 是一个 DataStream，我们可以看到构造函数中，生成了一个 SourceTransformation，指代生成 DataStreamSource 的操作

### 执行 map 操作

```java
public <R> SingleOutputStreamOperator<R> map(MapFunction<T, R> mapper) {

	TypeInformation<R> outType = TypeExtractor.getMapReturnTypes(clean(mapper), getType(),
			Utils.getCallLocationName(), true);

	return transform("Map", outType, new StreamMap<>(clean(mapper)));
}
	
public <R> SingleOutputStreamOperator<R> transform(String operatorName, TypeInformation<R> outTypeInfo, OneInputStreamOperator<T, R> operator) {
	
	// read the output type of the input Transform to coax out errors about MissingTypeInfo
	transformation.getOutputType();
	
	OneInputTransformation<T, R> resultTransform = new OneInputTransformation<>(
			this.transformation,
			operatorName,
			operator,
			outTypeInfo,
			environment.getParallelism());
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	SingleOutputStreamOperator<R> returnStream = new SingleOutputStreamOperator(environment, resultTransform);
	
	getExecutionEnvironment().addOperator(resultTransform);
	
	return returnStream;
}
```

我们看到 map 操作中生成了一个 StreamMap，StreamMap 同样是一个 StreamOperator，然后调用了 transform 方法，在 transform 中根据传入的 StreamMap 生成了一个新的 OneInputTransformation，然后根据 OneInputTransformation 生成了一个新的 DataStream，`getExecutionEnvironment().addOperator(resultTransform)` 将
OneInputTransformation 存储到 StreamExecutionEnvironment 中，用于生成 StreamGraph

### printToErr 执行 sink 操作

```java
public DataStreamSink<T> printToErr() {
	PrintSinkFunction<T> printFunction = new PrintSinkFunction<>(true);
	return addSink(printFunction).name("Print to Std. Err");
}

public DataStreamSink<T> addSink(SinkFunction<T> sinkFunction) {

	// read the output type of the input Transform to coax out errors about MissingTypeInfo
	transformation.getOutputType();

	// configure the type if needed
	if (sinkFunction instanceof InputTypeConfigurable) {
		((InputTypeConfigurable) sinkFunction).setInputType(getType(), getExecutionConfig());
	}

	StreamSink<T> sinkOperator = new StreamSink<>(clean(sinkFunction));

	DataStreamSink<T> sink = new DataStreamSink<>(this, sinkOperator);

	getExecutionEnvironment().addOperator(sink.getTransformation());
	return sink;
}
```

printToErr 方法首先生成一个 PrintSinkFunction，这是一个 SinkFunction，提供一个 invoke 方法，用于输出到错误流，然后调用 addSink 方法

addSink 先生成 StreamOperator（StreamSink），然后根据这个 StreamOperator 生成 SinkTransformation（在 DataStreamSink 的构造函数中生成），最后根据 SinkTransformation 生成 DataStreamSink，同样，也需要将 SinkTransformation 存储到 StreamExecutionEnvironment 中

## 总结

总的来说，StreamExecutionEnvironment 提供了流任务执行的环境，DataStream 提供了流操作的载体，各种变化（例如 map/filter/flatMap）都是在 DataStream 上执行的，StreamTransformation 代表了创建一个 DataStream 的操作，StreamOperator 负责处理流元素，只有 OneInputTransformation 和 TwoInputTransformation 中包含 StreamOperator，其他的像 UnionTransformation/PartitionTransformation/SelectTransformation 等都是逻辑上的操作，最后都序列化成连接 StreamGraph 的边
	
