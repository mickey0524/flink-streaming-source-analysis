# flink 的 RecordWriterOutput 和 RecordWriter

上一篇文章[flink 的 OperatorChain](./docs/flink-operator-chain.md)讲到，OperatorChain 的构造函数会接受一个 recordWriters 参数，recordWriters 是一个 RecordWriter 类型的 List。OperatorChain 会为 chain 的每一条出边创建一个 RecordWriterOutput 实例，RecordWriterOutput 中包裹着 RecordWriter，今天我们就来看看这两个类

## OperatorChain 构造函数的 recordWriters 是如何生成的

我们来看一下 OperatorChain 构造函数的 recordWriters 参数是如何生成的，代码位于 `org.apache.flink.streaming.runtime.tasks.StreamTask.java` 中

可以看到，首先会从 StreamConfig 中获取 JobVertex 的所有出边，也就是 chain 的所有出边，同时获取 chain 中所有操作符的 StreamConfig，然后对每一条边调用 createRecordWriter 方法

createRecordWriter 会从 StreamEdge 上获取 StreamPartitioner，并从 StreamTask 的指向环境中获取 ResultPartitionWriter，然后调用   静态方法 `RecordWriter.createRecordWriter` 生成一个 RecordWriter 实例

每条边得到的 RecordWriter 组成的 list 就是 OperatorChain 构造函数的 recordWriters 参数

```java
public static <OUT> List<RecordWriter<SerializationDelegate<StreamRecord<OUT>>>> createRecordWriters(
		StreamConfig configuration,
		Environment environment) {
	List<RecordWriter<SerializationDelegate<StreamRecord<OUT>>>> recordWriters = new ArrayList<>();
	// 以下两个都是生成 JobGraph 的时候生成并写入 Config 的
	List<StreamEdge> outEdgesInOrder = configuration.getOutEdgesInOrder(environment.getUserClassLoader());
	Map<Integer, StreamConfig> chainedConfigs = configuration.getTransitiveChainedTaskConfigsWithSelf(environment.getUserClassLoader());

	for (int i = 0; i < outEdgesInOrder.size(); i++) {
		StreamEdge edge = outEdgesInOrder.get(i);
		recordWriters.add(
			createRecordWriter(
				edge,
				i,
				environment,
				environment.getTaskInfo().getTaskName(),
				chainedConfigs.get(edge.getSourceId()).getBufferTimeout()));
	}
	return recordWriters;
}

private static <OUT> RecordWriter<SerializationDelegate<StreamRecord<OUT>>> createRecordWriter(
		StreamEdge edge,
		int outputIndex,
		Environment environment,
		String taskName,
		long bufferTimeout) {
	// 边上的 partitioner，没有显式设置的话，会使用 ForwardPartitioner 或 RebalancePartitioner
	StreamPartitioner<OUT> outputPartitioner = (StreamPartitioner<OUT>) edge.getPartitioner();

	ResultPartitionWriter bufferWriter = environment.getWriter(outputIndex);

	RecordWriter<SerializationDelegate<StreamRecord<OUT>>> output =
		RecordWriter.createRecordWriter(bufferWriter, outputPartitioner, bufferTimeout, taskName);

	return output;
}
```

## RecordWriterOutput

### 重要属性

```java
private RecordWriter<SerializationDelegate<StreamElement>> recordWriter;  // RecordWriter

private SerializationDelegate<StreamElement> serializationDelegate;  // 序列化委托

private final StreamStatusProvider streamStatusProvider;  // 流状态提供者，使用的时候传入的是 OperatorChain

private final OutputTag outputTag;  // 偏侧输出 tag
```

### 方法

collect 用于 emit 一个 StreamRecord 或者 偏侧输出一个 StreamRecord，会调用 RecordWriter 的 emit 方法

emitWatermark 用于 emit 一个 Watermark，只有当前 OperatorChain 的流状态为 Active 的时候，才会调用 RecordWriter 的 broadcastEmit 方法，会广播到 writer 的每一个 channel 中

emitStreamStatus 和 emitWatermark 类似，用于将流状态的更改广播到 writer 的每一个 channel 中

emitLatencyMarker 用于将 emit 延迟标记，只需要随机发送到一个 channel 中就能知道网络中的延迟，因此调用 RecordWriter 的 randomEmit 方法

broadcastEvent 方法用于广播检查点 barrier 或检查点取消这类 event，调用 RecordWriter 的 broadcastEvent 方法

```java
@Override
public void collect(StreamRecord<OUT> record) {
	// 这个方法只负责 emit 主输出
	if (this.outputTag != null) {
		// we are only responsible for emitting to the main input
		return;
	}

	pushToRecordWriter(record);
}

@Override
public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {
	// 这个方法只负责 emit 侧边输出，且 output 需要相同
	if (this.outputTag == null || !this.outputTag.equals(outputTag)) {
		// we are only responsible for emitting to the side-output specified by our
		// OutputTag.
		return;
	}

	pushToRecordWriter(record);
}

// 将元素 emit 到缓冲区
private <X> void pushToRecordWriter(StreamRecord<X> record) {
	serializationDelegate.setInstance(record);

	try {
		recordWriter.emit(serializationDelegate);
	}
	catch (Exception e) {
		throw new RuntimeException(e.getMessage(), e);
	}
}

@Override
// 广播 emit watermark
public void emitWatermark(Watermark mark) {
	watermarkGauge.setCurrentWatermark(mark.getTimestamp());
	serializationDelegate.setInstance(mark);

	if (streamStatusProvider.getStreamStatus().isActive()) {
		try {
			recordWriter.broadcastEmit(serializationDelegate);
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}
	}
}

// 广播流状态
public void emitStreamStatus(StreamStatus streamStatus) {
	serializationDelegate.setInstance(streamStatus);

	try {
		recordWriter.broadcastEmit(serializationDelegate);
	}
	catch (Exception e) {
		throw new RuntimeException(e.getMessage(), e);
	}
}

@Override
// 延迟 marker，随机发送
public void emitLatencyMarker(LatencyMarker latencyMarker) {
	serializationDelegate.setInstance(latencyMarker);

	try {
		recordWriter.randomEmit(serializationDelegate);
	}
	catch (Exception e) {
		throw new RuntimeException(e.getMessage(), e);
	}
}

public void broadcastEvent(AbstractEvent event) throws IOException {
	recordWriter.broadcastEvent(event);
}

public void flush() throws IOException {
	recordWriter.flushAll();
}

@Override
public void close() {
	recordWriter.close();
}
```

## RecordWriter

在 StreamTask 的 createRecordWriter 方法中，调用了 RecordWriter 的 createRecordWriter 方法创建了一个 RecordWriter 实例，RecordWriter 是 flink 中真正用于网络 io 的类，位于 `org.apache.flink.runtime.io.network.api.writer.RecordWriter.java`，不在 streaming 目录下

### 构造函数

构造函数接受四个参数，writer 是一个 ResultPartitionWriter，用来进行网络 io，channelSelector 其实就是 StreamPartitioner，用于决定流元素写入哪个 channel，timeout 是设置的 bufferTimeout， bufferTimeout 为 0，代表不缓存，每一条记录直接 flush，当 timeout 为 -1 的时候，按照 env 那里设置的 bufferTimeout 来，默认情况下，100 ms

RecordWriter 从 writer 中获取 numberOfChannels，指代下游有多少个 channel，然后调用 `this.channelSelector.setup` 将 channel 的数目写入 StreamPartitioner，serializer 用于序列化 StreamElement，当 timeout 不为 0 和 -1 的时候，RecordWriter 会创建一个常驻线程，用于定时 flush writer 中的流元素

```java
public RecordWriter(
		ResultPartitionWriter writer,
		ChannelSelector<T> channelSelector,
		long timeout,
		String taskName) {
	this.targetPartition = writer;
	this.channelSelector = channelSelector;
	this.numberOfChannels = writer.getNumberOfSubpartitions();
	this.channelSelector.setup(numberOfChannels);

	this.serializer = new SpanningRecordSerializer<T>();

	this.flushAlways = (timeout == 0);  // bufferTimeout 为 0，代表不缓存，每一条记录直接 flush
	// 当 timeout 为 -1 的时候，按照 env 那里设置的 bufferTimeout 来
	// 默认情况下，100 ms
	if (timeout == -1 || timeout == 0) {
		outputFlusher = Optional.empty();
	} else {
		// 如果设置了 bufferTimeout，将启动一个线程
		// Thread.sleep 等待对应的时间
		// 然后 flushAll
		String threadName = taskName == null ?
			DEFAULT_OUTPUT_FLUSH_THREAD_NAME :
			DEFAULT_OUTPUT_FLUSH_THREAD_NAME + " for " + taskName;

		outputFlusher = Optional.of(new OutputFlusher(threadName, timeout));
		outputFlusher.get().start();
	}
}
```

### OutputFlusher

OutputFlusher 继承 Thread，用于定时 flush writer 中的流元素，每次 flush 过后，都会调用 `Thread.sleep(timeout)`

```java
private class OutputFlusher extends Thread {

	private final long timeout;

	private volatile boolean running = true;

	OutputFlusher(String name, long timeout) {
		super(name);
		setDaemon(true);
		this.timeout = timeout;
	}

	public void terminate() {
		running = false;
		interrupt();
	}

	@Override
	public void run() {
		try {
			while (running) {
				try {
					Thread.sleep(timeout);
				} catch (InterruptedException e) {
					if (running) {
						throw new Exception(e);
					}
				}

				flushAll();
			}
		} catch (Throwable t) {
			notifyFlusherException(t);
		}
	}
}
```

### createRecordWriter

createRecordWriter 是一个静态方法，用于调用构造函数得到一个 RecordWriter 实例

```java
public static RecordWriter createRecordWriter(
		ResultPartitionWriter writer,
		ChannelSelector channelSelector,
		long timeout,
		String taskName) {
	// channelSelector 就是设置的各种 Partitioner
	// 如果是 BroadcastPartitioner，返回 BroadcastRecordWriter
	if (channelSelector.isBroadcast()) {
		return new BroadcastRecordWriter<>(writer, channelSelector, timeout, taskName);
	} else {
		return new RecordWriter<>(writer, channelSelector, timeout, taskName);
	}
}
```

### emit

emit 方法会调用 `channelSelector.selectChannel` 方法决定 record 写入哪个下标的 channel，然后序列化元素，并调用 copyFromSerializerToTargetChannel 写入对应 channel 的缓冲区 

```java
public void emit(T record) throws IOException, InterruptedException {
	emit(record, channelSelector.selectChannel(record));
}

private void emit(T record, int targetChannel) throws IOException, InterruptedException {
	serializer.serializeRecord(record);

	if (copyFromSerializerToTargetChannel(targetChannel)) {
		serializer.prune();
	}
}
```

### broadcastEmit

broadcastEmit 方法会轮询所有的 channel，通通写入，broadcastEvent 和此方法类似

```java
public void broadcastEmit(T record) throws IOException, InterruptedException {
	serializer.serializeRecord(record);

	boolean pruneAfterCopying = false;
	for (int channel : broadcastChannels) {
		if (copyFromSerializerToTargetChannel(channel)) {
			pruneAfterCopying = true;
		}
	}
}
```

### randomEmit

randomEmit 随机选择一个 channel 写入

```java
public void randomEmit(T record) throws IOException, InterruptedException {
	emit(record, rng.nextInt(numberOfChannels));
}
```

## 总结

这篇文章我们介绍了 flink 的 RecordWriter 和 RecordWriterOutput，希望大家有所收获

