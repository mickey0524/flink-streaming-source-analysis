# flink 的 StreamInputProcessor

这篇文章来讲解一下 flink 的 StreamInputProcessor，我们之前讲过，flink 流程序中的数据流动分为两块，OperatorChain 内部以及 OperatorChain 之间的流动，OperatorChain 到链外的出边通过 RecordWriter 将数据 emit 出去，StreamInputProcessor 用于接收这部分数据

## 重要属性

```java
// 所有 channel 的 record 反序列化工具
private final RecordDeserializer<DeserializationDelegate<StreamElement>>[] recordDeserializers;

// 当前 channel 的 record 反序列化工具
private RecordDeserializer<DeserializationDelegate<StreamElement>> currentRecordDeserializer;

// StreamElement 的反序列化工具
private final DeserializationDelegate<StreamElement> deserializationDelegate;

// 用于处理检查点的 barrier
private final CheckpointBarrierHandler barrierHandler;

// 操作符 emit 时候 synchronized 加锁的对象
private final Object lock;

// 控制 Watermark 和流状态何时被 emit 给下游
private StatusWatermarkValve statusWatermarkValve;

// 需要处理的输入通道数量
private final int numInputChannels;

// 当前从哪个 channel 中获取 BufferOrEvent
private int currentChannel = -1;

// 用于在流状态更改的时候通知链上的操作符，OperatorChain 就是 StreamStatusMaintainer
private final StreamStatusMaintainer streamStatusMaintainer;

// 操作符
private final OneInputStreamOperator<IN, ?> streamOperator;

// StreamInputProcessor 是否在运行，内部也有 while 循环
private boolean isFinished;
```

## processInput 方法

processInput 会在 `OneInputStreamTask.java` 中被调用（之后的文章会讲解），用于调用 CheckpointBarrierHandler 的 getNextNonBlocked 方法，处理每个通道到来的流元素

从👇的代码可以看到，processInput 方法会调用 barrierHandler 的 getNextNonBlocked 方法获取 bufferOrEvent，然后从 bufferOrEvent 中获取 currentChannel，设置 currentRecordDeserializer，并将 bufferOrEvent 写入 currentRecordDeserializer

processInput 从 currentRecordDeserializer 中获取 StreamElement，当 StreamElement 是 Watermark 或 StreamStatus 的时候，交由 StatusWatermarkValve 控制是否 emit（因为存在多个 channel，Watermark 和 StreamStatus 由多个 channel 共同控制），当接收到延迟标记的时候，调用操作符的 processLatencyMarker 方法，当接收到 StreamRecord 的时候，调用操作符的 processElement 方法，这样使得流元素流入 OperatorChain

```java
public boolean processInput() throws Exception {
	// 如果已经结束了，直接返回
	if (isFinished) {
		return false;
	}

	while (true) {
		if (currentRecordDeserializer != null) {
			DeserializationResult result = currentRecordDeserializer.getNextRecord(deserializationDelegate);

			if (result.isBufferConsumed()) {
				currentRecordDeserializer.getCurrentBuffer().recycleBuffer();
				currentRecordDeserializer = null;
			}

			if (result.isFullRecord()) {
				StreamElement recordOrMark = deserializationDelegate.getInstance();

				if (recordOrMark.isWatermark()) {
					// 处理 watermark
					statusWatermarkValve.inputWatermark(recordOrMark.asWatermark(), currentChannel);
					continue;
				} else if (recordOrMark.isStreamStatus()) {
					// handle stream status
					statusWatermarkValve.inputStreamStatus(recordOrMark.asStreamStatus(), currentChannel);
					continue;
				} else if (recordOrMark.isLatencyMarker()) {
					// 处理延迟 marker
					synchronized (lock) {
						streamOperator.processLatencyMarker(recordOrMark.asLatencyMarker());
					}
					continue;
				} else {
					// 处理 StreamRecord
					StreamRecord<IN> record = recordOrMark.asRecord();
					synchronized (lock) {
						numRecordsIn.inc();
						streamOperator.setKeyContextElement1(record);
						streamOperator.processElement(record);
					}
					return true;
				}
			}
		}
		// 更新 currentChannel
		final BufferOrEvent bufferOrEvent = barrierHandler.getNextNonBlocked();
		if (bufferOrEvent != null) {
			if (bufferOrEvent.isBuffer()) {
				currentChannel = bufferOrEvent.getChannelIndex();
				currentRecordDeserializer = recordDeserializers[currentChannel];
				currentRecordDeserializer.setNextBuffer(bufferOrEvent.getBuffer());
			}
			else {
				// Event received
				final AbstractEvent event = bufferOrEvent.getEvent();
				if (event.getClass() != EndOfPartitionEvent.class) {
					// 接收到预期之外的事件
					throw new IOException("Unexpected event: " + event);
				}
			}
		}
		else {
			isFinished = true;
			if (!barrierHandler.isEmpty()) {
				throw new IllegalStateException("Trailing data in checkpoint barrier handler.");
			}
			return false;
		}
	}
}
```

## ForwardingValveOutputHandler

ForwardingValveOutputHandler 实现了 `StatusWatermarkValve.ValveOutputHandler` 接口，当 StatusWatermarkValve 判断可以 emit 新的 Watermark／StreamStatus 给下游的时候，调用 ForwardingValveOutputHandler 的 handleWatermark／handleStreamStatus 方法

```java
private class ForwardingValveOutputHandler implements StatusWatermarkValve.ValveOutputHandler {
	private final OneInputStreamOperator<IN, ?> operator;
	private final Object lock;

	private ForwardingValveOutputHandler(final OneInputStreamOperator<IN, ?> operator, final Object lock) {
		this.operator = checkNotNull(operator);
		this.lock = checkNotNull(lock);
	}

	@Override
	public void handleWatermark(Watermark watermark) {
		try {
			synchronized (lock) {
				watermarkGauge.setCurrentWatermark(watermark.getTimestamp());
				operator.processWatermark(watermark);
			}
		} catch (Exception e) {
			throw new RuntimeException("Exception occurred while processing valve output watermark: ", e);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void handleStreamStatus(StreamStatus streamStatus) {
		try {
			synchronized (lock) {
				streamStatusMaintainer.toggleStreamStatus(streamStatus);
			}
		} catch (Exception e) {
			throw new RuntimeException("Exception occurred while processing valve output stream status: ", e);
		}
	}
}
```

## StatusWatermarkValve

StatusWatermarkValue 控制 Watermark 和 StreamStatus emit 到下游的逻辑

### 属性和构造函数

```java
private final ValveOutputHandler outputHandler;

/**
 * 所有输入通道的当前状态数组
 */
private final InputChannelStatus[] channelStatuses;

// value emit 的上一个 watermark
private long lastOutputWatermark;

// value emit 的上一个 StreamStatus
private StreamStatus lastOutputStreamStatus;

/**
 * 构造函数，返回一个 StatusWatermarkValve 实例
 * 初始化 InputChannelStatus 数组
 */
public StatusWatermarkValve(int numInputChannels, ValveOutputHandler outputHandler) {
	checkArgument(numInputChannels > 0);
	this.channelStatuses = new InputChannelStatus[numInputChannels];
	for (int i = 0; i < numInputChannels; i++) {
		channelStatuses[i] = new InputChannelStatus();
		channelStatuses[i].watermark = Long.MIN_VALUE;
		channelStatuses[i].streamStatus = StreamStatus.ACTIVE;
		channelStatuses[i].isWatermarkAligned = true;
	}

	this.outputHandler = checkNotNull(outputHandler);

	this.lastOutputWatermark = Long.MIN_VALUE;
	this.lastOutputStreamStatus = StreamStatus.ACTIVE;
}
```

### InputChannelStatus

InputChannelStatus 存储了一个 channel 当前的状态，watermark 存储当前 channel 的 Watermark，streamStatus 存储当前 channel 的 StreamStatus，isWatermarkAligned 指代当前 channel 是否对齐

```java
protected static class InputChannelStatus {
	protected long watermark;
	protected StreamStatus streamStatus;
	protected boolean isWatermarkAligned;

	/**
	 * Utility to check if at least one channel in a given array of input channels is active.
	 */
	/**
	 * 检查是否至少有一个通道是 active 的
	 */
	private static boolean hasActiveChannels(InputChannelStatus[] channelStatuses) {
		for (InputChannelStatus status : channelStatuses) {
			if (status.streamStatus.isActive()) {
				return true;
			}
		}
		return false;
	}
}
```

### inputWatermark

inputWatermark 的注释非常详细

```java
public void inputWatermark(Watermark watermark, int channelIndex) {
	// 当全部的输入通道或者执行下标的输入通道空闲的时候，忽略输入的水印
	if (lastOutputStreamStatus.isActive() && channelStatuses[channelIndex].streamStatus.isActive()) {
		long watermarkMillis = watermark.getTimestamp();

		if (watermarkMillis > channelStatuses[channelIndex].watermark) {
			// 当水印大于当前通道的水印，更新
			channelStatuses[channelIndex].watermark = watermarkMillis;

			// 更新对齐状态
			if (!channelStatuses[channelIndex].isWatermarkAligned && watermarkMillis >= lastOutputWatermark) {
				channelStatuses[channelIndex].isWatermarkAligned = true;
			}

			// 现在，尝试在所有对齐的通道上找到新的最小水印
			findAndOutputNewMinWatermarkAcrossAlignedChannels();
		}
	}
}

// 尝试在所有对齐的通道上找到新的最小水印
private void findAndOutputNewMinWatermarkAcrossAlignedChannels() {
	long newMinWatermark = Long.MAX_VALUE;
	boolean hasAlignedChannels = false;

	for (InputChannelStatus channelStatus : channelStatuses) {
		if (channelStatus.isWatermarkAligned) {
			hasAlignedChannels = true;
			newMinWatermark = Math.min(channelStatus.watermark, newMinWatermark);
		}
	}

	// 更新全局的 watermark
	if (hasAlignedChannels && newMinWatermark > lastOutputWatermark) {
		lastOutputWatermark = newMinWatermark;
		outputHandler.handleWatermark(new Watermark(lastOutputWatermark));
	}
}
```

### inputStreamStatus

inputStreamStatus 的注释非常详细

```java
/**
 * 将 StreamStatus 传入 value，有可能触发新的 StreamStatus 的生成或者新的 Watermark 的生成
 */
public void inputStreamStatus(StreamStatus streamStatus, int channelIndex) {
	// 仅考虑流状态输入，这将导致输入通道的状态更改
	if (streamStatus.isIdle() && channelStatuses[channelIndex].streamStatus.isActive()) {
		// 将当前 channel 的状态从 active 变为 idle
		channelStatuses[channelIndex].streamStatus = StreamStatus.IDLE;

		// 当前 channel 空闲了，因此不对齐了
		channelStatuses[channelIndex].isWatermarkAligned = false;

		// 如果所有的输入通道都空闲了，我们需要输出一个 idle 流状态
		if (!InputChannelStatus.hasActiveChannels(channelStatuses)) {

			// 既然所有输入通道都是空闲的，没有通道可以继续更新其水印，我们应该“冲洗”所有通道上的所有水印
			// 实际上，这意味着在所有通道上发出最大水印作为新水印。此外，由于我们总是尝试更新最小水印（findAndOutputNewMinWatermarkAcrossAlignedChannels方法）
			// 因此只有刚刚变为空闲的最后一个活动通道的水印是当前最小水印，我们需要执行刷新
			if (channelStatuses[channelIndex].watermark == lastOutputWatermark) {
				findAndOutputMaxWatermarkAcrossAllChannels();
			}

			lastOutputStreamStatus = StreamStatus.IDLE;
			outputHandler.handleStreamStatus(lastOutputStreamStatus);
		} else if (channelStatuses[channelIndex].watermark == lastOutputWatermark) {
			// 如果刚刚变为空闲的信道的水印等于最后的输出水印（前一个整体最小水印）
			// 我们可能能够从剩余的对齐频道中找到新的最小水印
			findAndOutputNewMinWatermarkAcrossAlignedChannels();
		}
	} else if (streamStatus.isActive() && channelStatuses[channelIndex].streamStatus.isIdle()) {
		// 将当前 channel 的状态从 idle 变为 active
		channelStatuses[channelIndex].streamStatus = StreamStatus.ACTIVE;

		// 重新对齐
		if (channelStatuses[channelIndex].watermark >= lastOutputWatermark) {
			channelStatuses[channelIndex].isWatermarkAligned = true;
		}

		// 有一个通道活跃了，整体也活跃了
		if (lastOutputStreamStatus.isIdle()) {
			lastOutputStreamStatus = StreamStatus.ACTIVE;
			outputHandler.handleStreamStatus(lastOutputStreamStatus);
		}
	}
}

/**
 * 从所有 channel 中找出 watermark 最大的
 */
private void findAndOutputMaxWatermarkAcrossAllChannels() {
	long maxWatermark = Long.MIN_VALUE;

	for (InputChannelStatus channelStatus : channelStatuses) {
		maxWatermark = Math.max(channelStatus.watermark, maxWatermark);
	}

	if (maxWatermark > lastOutputWatermark) {
		lastOutputWatermark = maxWatermark;
		outputHandler.handleWatermark(new Watermark(lastOutputWatermark));
	}
}
```

## 总结

这篇文章我们讲解了 flink 的 StreamInputProcessor，StreamInputProcessor 适用于接收 RecordWriter output 的数据，然后将数据写入 OperatorChain，希望对大家有帮助
