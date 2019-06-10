# flink 的 CheckpointBarrierHandler

今天这篇文章我们来讲一下 flink 的 CheckpointBarrierHandler，CheckpointBarrierHandler 会根据 flink 的两种工作模式 —— EXACTLY\_ONCE 和 AT\_LEAST\_ONCE 选取不同的实现类，CheckpointBarrierHandler 用于通知 StreamTask 检查点的完成和取消，StreamTask 再通过 OperatorChain 传递事件给所有的操作符（后续文章会讲解）

## CheckpointBarrierHandler

CheckpointBarrierHandler 是一个接口，BarrierBuffer 和 BarrierTracker 是两个实现类，BarrierBuffer 用于 EXACTLY\_ONCE 模式，BarrierTracker 用于 AT\_LEAST\_ONCE 模式

```java
public interface CheckpointBarrierHandler {

	/**
	 * 返回运算符可能使用的下一个 BufferOrEvent
	 * 此调用将阻塞，直到下一个 BufferOrEvent 可用，或者直到确定流已完成为止
	 */
	BufferOrEvent getNextNonBlocked() throws Exception;

	/**
	 * 一旦收到检查点的所有检查点障碍，就会通知注册任务
	 */
	void registerCheckpointEventHandler(AbstractInvokable task);

	/**
	 * 清理所有内部资源
	 */
	void cleanup() throws IOException;

	/**
	 * 检查屏障处理程序是否在内部缓冲了任何数据
	 */
	boolean isEmpty();

	/**
	 * 获取最新对齐所用的时间，以纳秒为单位
	 * 如果当前正在进行对齐，则它将返回到目前为止在当前对齐中花费的时间
	 * 	
	 * 通俗点讲，其实就是本次检查点耗费了多少时间
	 */
	long getAlignmentDurationNanos();
}
```

## InputProcessorUtil

在 StreamInputProcessor 和 StreamTwoInputProcessor 中，通过调用 `InputProcessorUtil.createCheckpointBarrierHandler` 来创建 CheckpointBarrierHandler 实例

当 checkpointMode 为 AT_LEAST_ONCE 的时候，创建 BarrierTracker 实例，反之，创建 BarrierBuffer 实例，当网络模型可信的时候，使用 CachedBufferBlocker 缓存 BufferOrEvent，反之使用 BufferSpiller，最后，调用 `barrierHandler.registerCheckpointEventHandler` 方法注册检查点 barrier 接收完毕后的回调实例

```java
public static CheckpointBarrierHandler createCheckpointBarrierHandler(
		StreamTask<?, ?> checkpointedTask,
		CheckpointingMode checkpointMode,
		IOManager ioManager,
		InputGate inputGate,
		Configuration taskManagerConfig) throws IOException {

	CheckpointBarrierHandler barrierHandler;
	// 当检查点模式为 EXACTLY_ONCE 的时候
	if (checkpointMode == CheckpointingMode.EXACTLY_ONCE) {
		long maxAlign = taskManagerConfig.getLong(TaskManagerOptions.TASK_CHECKPOINT_ALIGNMENT_BYTES_LIMIT);
		
		// 当网络模型可信的时候，选用 CachedBufferBlocker 缓存 BufferOrEvent
		if (taskManagerConfig.getBoolean(TaskManagerOptions.NETWORK_CREDIT_MODEL)) {
			barrierHandler = new BarrierBuffer(inputGate, new CachedBufferBlocker(inputGate.getPageSize()), maxAlign);
		} else {
			barrierHandler = new BarrierBuffer(inputGate, new BufferSpiller(ioManager, inputGate.getPageSize()), maxAlign);
		}
	}
	// 当检查点模式为 AT_LEAST_ONCE 的时候
	else if (checkpointMode == CheckpointingMode.AT_LEAST_ONCE) {
		barrierHandler = new BarrierTracker(inputGate);
	} else {
		throw new IllegalArgumentException("Unrecognized Checkpointing Mode: " + checkpointMode);
	}

	if (checkpointedTask != null) {
		// 用于访问 StreamTask 中的 triggerCheckpointOnBarrier 方法
		barrierHandler.registerCheckpointEventHandler(checkpointedTask);
	}

	return barrierHandler;
}
```

## BarrierTracker

BarrierTracker 不会阻塞通道，即使通道接收到了 barrier，同样允许流元素流下去，因此，在检查点恢复的时候，下游可能会收到重复的数据，因此只能被用在 AT\_LEAST\_ONCE 的工作模式下

### 重要属性

```java
// BarrierTracker 最多保存 MAX_CHECKPOINTS_TO_TRACK 个检查点
private static final int MAX_CHECKPOINTS_TO_TRACK = 50;

// 从 inputGate 接收网络 io 流入的元素
private final InputGate inputGate;

// inputGate 中 channel 的数量，一旦检查点收到了这么多 barriers，检查点被认为完成
private final int totalNumberOfInputChannels;

// 保存当前流入 BarrierTracker 的检查点
private final ArrayDeque<CheckpointBarrierCount> pendingCheckpoints;

// 检查点完成的时候触发的回调，也就是 StreamTask
private AbstractInvokable toNotifyOnCheckpoint;

// 到目前为止遇到的最大检查点 ID
private long latestPendingCheckpointID = -1;
```

### getNextNonBlocked

getNextNonBlocked 方法首先从 inputGate 中获取下一个元素，如果到来的是 buffer，直接返回给 StreamInputProcessor 或 StreamTwoInputProcessor 处理，如果到来的是检查点 barrier，调用 processBarrier 方法，如果到来的是检查点取消事件，调用 processCheckpointAbortBarrier 方法

```java
public BufferOrEvent getNextNonBlocked() throws Exception {
	while (true) {
		Optional<BufferOrEvent> next = inputGate.getNextBufferOrEvent();
		if (!next.isPresent()) {
			// buffer or input exhausted
			// 缓存或输入耗尽
			return null;
		}

		BufferOrEvent bufferOrEvent = next.get();
		// 如果是 buffer 的话
		if (bufferOrEvent.isBuffer()) {
			return bufferOrEvent;
		}
		// 收到了检查点 barrier
		else if (bufferOrEvent.getEvent().getClass() == CheckpointBarrier.class) {
			processBarrier((CheckpointBarrier) bufferOrEvent.getEvent(), bufferOrEvent.getChannelIndex());
		}
		// 收到了取消检查点
		else if (bufferOrEvent.getEvent().getClass() == CancelCheckpointMarker.class) {
			processCheckpointAbortBarrier((CancelCheckpointMarker) bufferOrEvent.getEvent(), bufferOrEvent.getChannelIndex());
		}
		else {
			// some other event
			// 一些其他的 event
			return bufferOrEvent;
		}
	}
}
```

### CheckpointBarrierCount

CheckpointBarrierCount 用于保存检查点到来了多少个 barrier，也就是多少个 channel 流入了 barrier

从下方代码中可以看到，checkpointId 存储了检查点的 id，barrierCount 存储了该检查点到来的 barrier 数量，aborted 指代该检查点是否被取消了，只有未取消的检查点完成的时候才能触发 StreamTask

```java
private static final class CheckpointBarrierCount {
	
	private final long checkpointId;
	
	private int barrierCount;
	
	private boolean aborted;
	
	CheckpointBarrierCount(long checkpointId) {
		this.checkpointId = checkpointId;
		this.barrierCount = 1;
	}
	
	public long checkpointId() {
		return checkpointId;
	}
	
	public int incrementBarrierCount() {
		return ++barrierCount;
	}
	
	public boolean isAborted() {
		return aborted;
	}
	
	public boolean markAborted() {
		boolean firstAbort = !this.aborted;  // 是否是第一次 abort
		this.aborted = true;
		return firstAbort;
	}
	
	@Override
	public String toString() {
		return isAborted() ?
			String.format("checkpointID=%d - ABORTED", checkpointId) :
			String.format("checkpointID=%d, count=%d", checkpointId, barrierCount);
	}
}
```

### processBarrier

processBarrier 从 receivedBarrier 中获取检查点 id，如果 inputGate 只有一个通道，说明检查点完成，触发回调。否则，遍历 pendingCheckpoints，比对 pendingCheckpoints 中 CheckpointBarrierCount 的 checkpointId 和 barrierId，匹配成功后 break 出循环

如果 barrierId 之前就存在，对应的 CheckpointBarrierCount 的 barrierCount 自增，如果 barrierCount 和 totalNumberOfInputChannels 相等，说明检查点完成，将该检查点和之前的检查点全部出队列，并调用检查点成功的回调

如果 barrierId 不存在，只有 barrier 大于 latestPendingCheckpointID 的时候才处理（小于说明是之前已经被 poll 出队列的检查点的 barrier，直接丢弃），说明来了一个新的检查点，将其加入等待队列

```java
private void processBarrier(CheckpointBarrier receivedBarrier, int channelIndex) throws Exception {
	// 获取检查点 ID
	final long barrierId = receivedBarrier.getId();

	// 单通道跟踪器的快速路径，只有一个通道的话，接到一个 Barrier，就说明检查点完成
	if (totalNumberOfInputChannels == 1) {
		notifyCheckpoint(barrierId, receivedBarrier.getTimestamp(), receivedBarrier.getCheckpointOptions());
		return;
	}

	// 在等待队列中寻找检查点 barrier
	CheckpointBarrierCount cbc = null;
	int pos = 0;

	for (CheckpointBarrierCount next : pendingCheckpoints) {
		if (next.checkpointId == barrierId) {
			cbc = next;
			break;
		}
		pos++;
	}

	// 检查点之前就存在
	if (cbc != null) {
		// 给 count 加一，判断是否完成了 checkpoint
		int numBarriersNew = cbc.incrementBarrierCount();
		if (numBarriersNew == totalNumberOfInputChannels) {
			// 检查点可以被触发（或被中止并且已经看到所有障碍）首先，删除此检查点以及所有先前的待处理检查点（现在已包含）
			for (int i = 0; i <= pos; i++) {
				pendingCheckpoints.pollFirst();
			}

			// 通知监听者
			if (!cbc.isAborted()) {
				notifyCheckpoint(receivedBarrier.getId(), receivedBarrier.getTimestamp(), receivedBarrier.getCheckpointOptions());
			}
		}
	}
	// 检查点之前不存在，是一个全新的 checkpoint
	else {
		// 该 checkpointID 的第一个屏障
		// 添加该 checkpointID 当其 id 大于最近的一个 checkpoint
		// 否则，无论如何都不能成功获得该ID的检查点
		if (barrierId > latestPendingCheckpointID) {
			latestPendingCheckpointID = barrierId;
			pendingCheckpoints.addLast(new CheckpointBarrierCount(barrierId));

			// 确保我们不能同时跟踪多个检查点
			if (pendingCheckpoints.size() > MAX_CHECKPOINTS_TO_TRACK) {
				pendingCheckpoints.pollFirst();
			}
		}
	}
}
```

### processCheckpointAbortBarrier

processCheckpointAbortBarrier 同样从 barrier 中获取 checkpointId，如果 inputGate 只有一个通道，直接调用 notifyAbort 方法通知检查点取消，否则去等待队列中寻找该 checkpointId 的位置，并把 CheckpointBarrierCount 的 checkpointId 小于当前 checkpointId 的 item 全部 remove

如果当前 checkpointId 存在于队列中，同时这是第一次 abort，调用 notifyAbort 方法；如果当前 checkpointId 不在队列中且 id 比 latestPendingCheckpointID 大，说明是一个新的检查点，直接调用 notifyAbort 方法，然后新建一个 CheckpointBarrierCount 加入队列

```java
private void processCheckpointAbortBarrier(CancelCheckpointMarker barrier, int channelIndex) throws Exception {
	final long checkpointId = barrier.getCheckpointId();

	// 单通道跟踪器的快速通道
	if (totalNumberOfInputChannels == 1) {
		notifyAbort(checkpointId);
		return;
	}
	// 找到该 checkpointID 在队列中的位置
	// 并且执行该 checkpointID 之前所有 checkpoint 的 notifyAbort 方法
	CheckpointBarrierCount cbc;
	while ((cbc = pendingCheckpoints.peekFirst()) != null && cbc.checkpointId() < checkpointId) {
		pendingCheckpoints.removeFirst();

		if (cbc.markAborted()) {
			// 如果尚未完成，则中止对应的检查点
			notifyAbort(cbc.checkpointId());
		}
	}

	if (cbc != null && cbc.checkpointId() == checkpointId) {
		// 确保检查点被标记为中止
		if (cbc.markAborted()) {
			// 这是检查点第一次中止 - 通知
			notifyAbort(checkpointId);
		}

		// 我们依旧对 barrier 计数，并且在所有的 barrier 到来之后，从等待队列中将其删除
		if (cbc.incrementBarrierCount() == totalNumberOfInputChannels) {
			// we can remove this entry
			pendingCheckpoints.removeFirst();
		}
	}
	else if (checkpointId > latestPendingCheckpointID) {
		notifyAbort(checkpointId);

		latestPendingCheckpointID = checkpointId;

		CheckpointBarrierCount abortedMarker = new CheckpointBarrierCount(checkpointId);
		abortedMarker.markAborted();
		pendingCheckpoints.addFirst(abortedMarker);

		// 我们已经删除了所有其他待处理的检查点障碍计数 - > 无需检查我们是否超过能跟踪的最大检查点数目
	} else {
		// trailing cancellation barrier which was already cancelled
	}
}
```

## BarrierBuffer

BarrierBuffer 会阻塞通道，当一个通道接收到检查点 barrier 后，流元素就不能流下去了，BarrierBuffer 会将流入阻塞通道的元素存储到 BufferOrEventSequence 中

### BufferOrEventSequence

BufferOrEventSequence 是一个队列，当通道阻塞的时候，用于缓存 inputGate 中获取的元素 —— BufferOrEvent，BufferOrEventSequence 的 getNext 方法和迭代器一样，依次返回队列中的元素

```java
public interface BufferOrEventSequence {

	/**
	 * 初始化队列
	 */
	void open();

	/**
	 * 从队列中获取下一个 BufferOrEvent
	 * 如果队列中没有其他元素，返回 null
	 */
	@Nullable
	BufferOrEvent getNext() throws IOException;

	/**
	 * 清空队列申请的资源
	 */
	void cleanup() throws IOException;

	/**
	 * 获取队列的大小
	 */
	long size();
}
```

### 🚧 Under Construction

### BufferSpiller

### CachedBufferBlocker

