# flink 中的 StreamPartitioner

这篇文章介绍一下 flink 的分区器，flink 在 StreamTask 输出元素的时候，RecordWriter 会通过分区器来精确的控制数据流向下游的哪个 task

## StreamPartitioner

StreamPartitioner 是 flink 所有流分区器的基类，其中有两个重要的方法

```java
public void setup(int numberOfChannels) {
	this.numberOfChannels = numberOfChannels;
}
```

setup 方法在 RecordWriter 初始化的时候被调用，用来设置 StreamPartitioner 能够选择下游 task 的数量

```java
int selectChannel(T record);
```

selectChannel 方法是需要具体的 partitioner 自行实现的，方法返回逻辑 channel 下标，下标表示给定的 record 应该被写入哪一个输出通道

## BroadcastPartitioner

BroadcastPartitioner 会将 StreamRecord 分发到下游每一个 task 中去，所以 selectChannel 方法会抛出异常，在 RecordWriter 中，会用 for 循环将元素输出到所有的下游 task

```java
public int selectChannel(SerializationDelegate<StreamRecord<T>> record) {
	throw new UnsupportedOperationException("Broadcast partitioner does not support select channels.");
}
```

## ForwardPartitioner

ForwardPartitioner 会将 StreamRecord 分发到 task 直接对应的下游 task 中去

```java
public int selectChannel(SerializationDelegate<StreamRecord<T>> record) {
	return 0;
}
```

在 `StreamingJobGraphGenerator.java` 中，当添加两个 JobVertex 中间的边的时候，如果边上的 Partitioner 是 ForwardPartitioner 的话，会采用 DistributionPattern.POINTWISE 分配模式，使得上游的 task 节点只能连接到一个对应的下游 task 节点，因此 selectChannel 返回 0

## GlobalPartitioner

GlobalPartitioner 会将 StreamRecord 全部分发到下游 channelIndex = 0 的 task 中去

```java
public int selectChannel(SerializationDelegate<StreamRecord<T>> record) {
	return 0;
}
```

## KeyGroupStreamPartitioner

KeyGroupStreamPartitioner 是 keyBy 操作内部使用的分区器，通过 KeySelector 获取 key，再根据 StreamRecord 的 key 将元素分发到不同的下游 task

```java
public int selectChannel(SerializationDelegate<StreamRecord<T>> record) {
	K key;
	try {
		key = keySelector.getKey(record.getInstance().getValue());
	} catch (Exception e) {
		throw new RuntimeException("Could not extract key from " + record.getInstance().getValue(), e);
	}
	return KeyGroupRangeAssignment.assignKeyToParallelOperator(key, maxParallelism, numberOfChannels);
}
```

## RebalancePartitioner

RebalancePartitioner 会将 StreamRecord 以轮询的方式分发到下游的 task 中，RebalancePartitioner 会随机选择一个 channel 作为初始 channel

```java
public void setup(int numberOfChannels) {
	super.setup(numberOfChannels);

	nextChannelToSendTo = ThreadLocalRandom.current().nextInt(numberOfChannels);
}

public int selectChannel(SerializationDelegate<StreamRecord<T>> record) {
	nextChannelToSendTo = (nextChannelToSendTo + 1) % numberOfChannels;
	return nextChannelToSendTo;
}
```

## RescalePartitioner

RescalePartitioner 以轮询的方式分发到下游的 task 中，需要注意的是（这也是 RescalePartitioner 和 RebalancePartitioner 的区别）：在 `StreamingJobGraphGenerator.java` 中，添加两个 JobVertex 中间的边的时候，如果边上的 Partitioner 是 RescalePartitioner 的话，会采用 DistributionPattern.POINTWISE 分配模式，仅仅将数据分配到下游节点的一个子集中。举个例子，如果上游 task 的并行度为 2，下游 task 的并行度为 4，一个上游 task 将会分配元素到两个下游 task，另一个上游 task 将分配元素到剩下的两个下游 task，从另一方面来说，如果上游 task 的并行度为 4，下游 task 的并行度为 2 的话，2 个上游 task 会分配元素到一个下游 task，另外两个上游 task 分配到剩下的一个下游 task 中

```java
public int selectChannel(SerializationDelegate<StreamRecord<T>> record) {
	if (++nextChannelToSendTo >= numberOfChannels) {
		nextChannelToSendTo = 0;
	}
	return nextChannelToSendTo;
}
```

## ShufflePartitioner

ShufflePartitioner 会将 StreamRecord 随机分发到下游的一个 task 中

```java
public int selectChannel(SerializationDelegate<StreamRecord<T>> record) {
	return random.nextInt(numberOfChannels);
}
```

## RecordWriter 中对 StreamPartitioner 的使用

### 设置 StreamPartitioner 的 numberOfChannels

```java
this.channelSelector = channelSelector;
this.numberOfChannels = writer.getNumberOfSubpartitions();
this.channelSelector.setup(numberOfChannels);
```

### 选择一个下游 task 分发元素

```java
emit(record, channelSelector.selectChannel(record));
```

### 广播将元素发送到所有的下游 task

```java
this.broadcastChannels = new int[numberOfChannels];

for (int channel : broadcastChannels) {
	if (copyFromSerializerToTargetChannel(channel)) {
		pruneAfterCopying = true;
	}
}
```

## 总结

今天我们讲解了一下 flink 中的 StreamPartitioner，只要是用于控制元素写入哪个下游 task，flink 中非常常用的 keyBy 操作也是通过 StreamPartitioner 实现的，在 DataStream 中，可以通过 forward/shuffle 等 API 显示的设置流的分区器