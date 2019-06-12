# flink 的 StreamTask

StreamTask 是所有 task 的基类，一个 task 是由 TaskManager 部署和执行的基本单位。每个 StreamTask 执行一个或多个 StreamOperator，这些 StreamOperators 组成了 StreamTask 的操作链，chain 在一起的 StreamOperators 在相同的线程中执行，因此也存在于相同的流分区中

StreamTask 中的 OperatorChain 包括一个 head operator 和多个链式相连的 operators

StreamTask 有几个子类 —— OneInputStreamTask、TwoInputStreamTask、SourceStreamTask、StreamIterationHead 和 StreamIterationTail，在 StreamGraph 的 addOperator 和 createIterationSourceAndSink 方法中，会根据操作符的类型，给 StreamNode 设置相应的 StreamTask

addOperator 方法

```java
if (operatorObject instanceof StoppableStreamSource) {
	addNode(vertexID, slotSharingGroup, coLocationGroup, StoppableSourceStreamTask.class, operatorObject, operatorName);
} else if (operatorObject instanceof StreamSource) {
	addNode(vertexID, slotSharingGroup, coLocationGroup, SourceStreamTask.class, operatorObject, operatorName);
} else {
	addNode(vertexID, slotSharingGroup, coLocationGroup, OneInputStreamTask.class, operatorObject, operatorName);
}
```

createIterationSourceAndSink 方法

```java
StreamNode source = this.addNode(sourceId,
	null,
	null,
	StreamIterationHead.class,
	null,
	"IterationSource-" + loopId);

StreamNode sink = this.addNode(sinkId,
	null,
	null,
	StreamIterationTail.class,
	null,
	"IterationSink-" + loopId);
```

`StreamTask.java` 中主要由 invoke 方法和检查点相关的方法两部分组成，今天这篇文章我们来讲一下 invoke 方法，检查点相关的方法会在下一篇文章中详细介绍

## StreamTask 主要属性（检查点相关的会在下篇文章讲解）

```java
// StreamOperator 的所有方法都需要靠 lock 同步，这样能保证我们不会并发调用影响检查点一致性的方法
private final Object lock = new Object();

// task 执行的操作符链
protected OperatorChain<OUT, OP> operatorChain;

// 操作符链的头部操作符
protected OP headOperator;

// task 的配置，其实就是 JobGraph 中 JobVertex 的 StreamConfig
protected final StreamConfig configuration;

// 持久存储检查点数据的外部存储
private CheckpointStorageWorkerView checkpointStorage;

// 内部 ProcessingTimeService 用于定义当前处理时间(default = System.currentTimeMillis()) 并注册将来要执行的任务的计时器
protected ProcessingTimeService timerService;

// 用于需要关闭的实例注册，然后在服务挂掉的时候，统一 close，释放资源
private final CloseableRegistry cancelables = new CloseableRegistry();

// 存储操作符链向链外 emit 数据使用的 RecordWriter
private final List<RecordWriter<SerializationDelegate<StreamRecord<OUT>>>> recordWriters;
```

## invoke 方法

invoke 方法会在 TaskManager 中被调用，用于启动流式任务，invoke 会按照如下的步骤执行

1. 创建基本的 utils（config等）并创建 OperatorChain
2. 调用 OperatorChain 中所有操作符的 setup 方法
3. 执行 StreamTask 的 init 方法，这是一个抽象方法，由子类负责实现
4. 调用 OperatorChain 中所有操作符的 initializeState 方法
5. 调用 OperatorChain 中所有操作符的 open 方法
6. 执行 StreamTask 的 run 方法，这也是一个抽象方法，多为一个 while 循环处理流元素
7. 调用 OperatorChain 中所有操作符的 close 方法
8. 调用 OperatorChain 中所有操作符的 dispose 方法
9. 执行 StreamTask 的 cleanup 方法，释放申请的资源

下面我们来看一下 invoke 的重要代码

```java
// 如果 clock 没有设置，使用 SystemProcessingTimeService
if (timerService == null) {
	ThreadFactory timerThreadFactory = new DispatcherThreadFactory(TRIGGER_THREAD_GROUP,
		"Time Trigger for " + getName(), getUserCodeClassLoader());

	timerService = new SystemProcessingTimeService(this, getCheckpointLock(), timerThreadFactory);
}

// 创建操作符链
operatorChain = new OperatorChain<>(this, recordWriters);
headOperator = operatorChain.getHeadOperator();

init();

synchronized (lock) {
	// 以下操作都受锁保护，以便在 initializeState() 注册一个计时器时避免竞争条件
	// 该计时器在调用 open() 前触发

	initializeState();
	openAllOperators();
}

isRunning = true;
run();

synchronized (lock) {
	closeAllOperators();

	timerService.quiesce();

	isRunning = false;
}

// 确保所有缓存的数据 flush
operatorChain.flushOutputs();

tryDisposeAllOperators();
disposed = true;
```

invoke 中调用的 initializeState 等方法，基本都是 for 循环调用操作符链中每个操作符对应的方法，这里给出 initializeState

```java
private void initializeState() throws Exception {

	StreamOperator<?>[] allOperators = operatorChain.getAllOperators();

	for (StreamOperator<?> operator : allOperators) {
		if (null != operator) {
			operator.initializeState();
		}
	}
}
```

## SourceStreamTask

SourceStreamTask 的 run 方法会调用 headOperator 的 run 方法

```java
protected void run() throws Exception {
	// chain 的头部操作符 run 起来
	headOperator.run(getCheckpointLock(), getStreamStatusMaintainer());
}
```

SourceStreamTask 中 headOperator 是 `StreamSource.java` 中定义的类的实例，其中的 run 方法，调用了 SourceFunction 的 run 方法，源源不断的产生流数据

## OneInputStreamTask

OneInputStreamTask 的 run 方法调用 StreamInputProcessor 的 processInput 方法，接收上游 RecordWriter emit 的数据，有关 StreamInputProcessor 可以参照[flink 的 StreamInputProcessor](./docs/flink-stream-input-processor.md)

```java
protected void run() throws Exception {
	// cache processor reference on the stack, to make the code more JIT friendly
	final StreamInputProcessor<IN> inputProcessor = this.inputProcessor;

	while (running && inputProcessor.processInput()) {
		// all the work happens in the "processInput" method
	}
}
```

## StreamIterationHead

StreamIterationHead 中的 headOperator 是反馈的头操作符，用于接收反馈尾传递过来的数据，然后 emit 给下游操作符，在 flink 中，反馈是通过 BlockingQueue 来实现的

run 方法首先获取 iterationId，在 StreamGraph 中，为迭代头、尾节点设置的 iterationId 为 `broker-${StreamTransformation.id}`，然后调用 createBrokerIdString 方法获取 brokerID，brokerID 是用于标识 iterationId 使用的 BlockingQueue，反馈头和反馈尾会得到相同的 brokerID，然后获取 iterationWaitTime，这个是在 DataStream 的 iterate 方法中设置的，如果超过 iterationWaitTime 没有反馈数据到来，run 方法停止

run 方法使用一个 while 循环，如果 iterationWaitTime > 0，调用 `dataChannel.poll(iterationWaitTime, TimeUnit.MILLISECONDS)`，反之调用 `dataChannel.take()`，接收到数据后，调用 output 方法，发送给所有的下游
 
```java
protected void run() throws Exception {

	final String iterationId = getConfiguration().getIterationId();  // broker-${StreamTransformation.id}

	final String brokerID = createBrokerIdString(getEnvironment().getJobID(), iterationId ,
			getEnvironment().getTaskInfo().getIndexOfThisSubtask());  // 生成 brokerID

	final long iterationWaitTime = getConfiguration().getIterationWaitTime();  // 获取反馈 waitTime
	final boolean shouldWait = iterationWaitTime > 0;  // 大于 0 表示显式设置了等待时间，waitTime 之内没有数据反馈，停止反馈

	// 创建一个 BlockingQueue
	final BlockingQueue<StreamRecord<OUT>> dataChannel = new ArrayBlockingQueue<StreamRecord<OUT>>(1);

	BlockingQueueBroker.INSTANCE.handIn(brokerID, dataChannel);

	try {
		RecordWriterOutput<OUT>[] outputs = (RecordWriterOutput<OUT>[]) getStreamOutputs();  // 获取流输出

		while (running) {
			// 如果设置了等待时机，则 BlockingQueue 最多等待 iterationWaitTime
			StreamRecord<OUT> nextRecord = shouldWait ?
				dataChannel.poll(iterationWaitTime, TimeUnit.MILLISECONDS) :
				dataChannel.take();

			if (nextRecord != null) {
				synchronized (getCheckpointLock()) {
					for (RecordWriterOutput<OUT> output : outputs) {
						output.collect(nextRecord);
					}
				}
			}
			else {
				break;
			}
		}
	}
	finally {
		// 等待时间过了，没有数据
		// 确保我们从代理中删除队列，以防止资源泄漏
		BlockingQueueBroker.INSTANCE.remove(brokerID);
	}
}

public static String createBrokerIdString(JobID jid, String iterationID, int subtaskIndex) {
	return jid + "-" + iterationID + "-" + subtaskIndex;
}
```

## StreamIterationTail

StreamIterationTail 和 StreamIterationHead 共同使用，从下方的代码中可以看到，StreamIterationTail 会得到 StreamIterationHead 中生成的 BlockingQueue，当接受到数据之后，会将数据写入 BlockingQueue

```java
final String iterationId = getConfiguration().getIterationId();  // broker-${StreamTransformation.id}

// 迭代头和迭代尾巴的 brokerID 相同，通过这个 brokerID 共享 BlockingQueue，简单的生产者消费者模式
final String brokerID = StreamIterationHead.createBrokerIdString(getEnvironment().getJobID(), iterationId,
		getEnvironment().getTaskInfo().getIndexOfThisSubtask());

final long iterationWaitTime = getConfiguration().getIterationWaitTime();

BlockingQueue<StreamRecord<IN>> dataChannel =
		(BlockingQueue<StreamRecord<IN>>) BlockingQueueBroker.INSTANCE.get(brokerID);

public void collect(StreamRecord<IN> record) {
	try {
		if (shouldWait) {
			// 如果设置了等待时间，则最多等待 iterationWaitTime
			dataChannel.offer(record, iterationWaitTime, TimeUnit.MILLISECONDS);
		}
		else {
			dataChannel.put(record);
		}
	} catch (InterruptedException e) {
		throw new RuntimeException(e);
	}
}
```

## 总结

这篇文章给大家讲解了一下 StreamTask，相信看过之后，大家对 flink 的流式任务如何执行能有一个更清晰的了解