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

### BufferSpiller

BufferSpiller 用文件存储元素

#### 属性和构造方法

```java
// header 字节数 (add 方法有用)
static final int HEADER_SIZE = 9;

// 选择下一个 spill 的目录的计数器，静态方法
private static final AtomicInteger DIRECTORY_INDEX = new AtomicInteger(0);

// 读回数据的缓冲区大小
private static final int READ_BUFFER_SIZE = 1024 * 1024;

// spill 的目录
private final File tempDir;

// spill 文件名前缀
private final String spillFilePrefix;

// 用于批量读取数据的缓冲区（用于 SpilledBufferOrEventSequence）
private final ByteBuffer readBuffer;

// 编码溢出标头的缓冲区
private final ByteBuffer headBuffer;

// 我们当前 spill 的文件
private File currentSpillFile;

// 我们当前 spill 的文件 channel
private FileChannel currentChannel;

// 页面大小，让这个阅读器实例化适当大小的内存段
private final int pageSize;

// 一个计数器，用于创建溢出文件的编号
private int fileCounter;

// 从上次反转以来，写入的字节数目
private long bytesWritten;

/**
 * 创建一个新的缓冲区 spiller，溢出到 I/O 管理器的临时目录之一
 */
public BufferSpiller(IOManager ioManager, int pageSize) throws IOException {
	this.pageSize = pageSize;

	// 分配缓冲区大小
	this.readBuffer = ByteBuffer.allocateDirect(READ_BUFFER_SIZE);
	this.readBuffer.order(ByteOrder.LITTLE_ENDIAN);

	this.headBuffer = ByteBuffer.allocateDirect(16);
	this.headBuffer.order(ByteOrder.LITTLE_ENDIAN);

	// 获取 spill 的目录
	File[] tempDirs = ioManager.getSpillingDirectories();
	this.tempDir = tempDirs[DIRECTORY_INDEX.getAndIncrement() % tempDirs.length];

	// 随机数生成一个前缀
	byte[] rndBytes = new byte[32];
	ThreadLocalRandom.current().nextBytes(rndBytes);
	this.spillFilePrefix = StringUtils.byteToHexString(rndBytes) + '.';

	// 创建 file 和 channel
	createSpillingChannel();
}
```

#### add 方法

add 方法将 BufferOrEvent 读取到 ByteBuffer 中，headBuffer 会写入 4 个字节的通道号，4 个字节的 contents 字节数，以及 1 个字节的 BufferOrEvent 类型，加起来 9 个字节，对应属性中的 HEADER_SIZE，最后 add 方法将 headBuffer 和 contents 先后写入文件中

```java
public void add(BufferOrEvent boe) throws IOException {
	try {
		ByteBuffer contents;
		if (boe.isBuffer()) {
			Buffer buf = boe.getBuffer();
			contents = buf.getNioBufferReadable();
		}
		else {
			contents = EventSerializer.toSerializedEvent(boe.getEvent());
		}

		headBuffer.clear();
		headBuffer.putInt(boe.getChannelIndex());  // 4 字节
		headBuffer.putInt(contents.remaining());  // 4 字节
		headBuffer.put((byte) (boe.isBuffer() ? 0 : 1));  // 1 字节，加起来 9 个字节，对应 HEADER_SIZE
		headBuffer.flip();

		bytesWritten += (headBuffer.remaining() + contents.remaining());

		FileUtils.writeCompletely(currentChannel, headBuffer);
		FileUtils.writeCompletely(currentChannel, contents);
	}
	finally {
		if (boe.isBuffer()) {
			boe.getBuffer().recycleBuffer();
		}
	}
}
```

#### rollOverReusingResources 方法和 rollOverWithoutReusingResources 方法

rollOverReusingResources 方法和 rollOverWithoutReusingResources 方法都是用来生成 SpilledBufferOrEventSequence 的，rollOverReusingResources 方法会重用 readBuffer，rollOverWithoutReusingResources 会重新生成一个 readBuffer

```java
public BufferOrEventSequence rollOverReusingResources() throws IOException {
	return rollOver(false);
}

public BufferOrEventSequence rollOverWithoutReusingResources() throws IOException {
	return rollOver(true);
}

private BufferOrEventSequence rollOver(boolean newBuffer) throws IOException {
	if (bytesWritten == 0) {
		return null;
	}

	ByteBuffer buf;
	// 如果 newBuffer 为 true，说明不能重用资源，需要重新分配
	if (newBuffer) {
		buf = ByteBuffer.allocateDirect(READ_BUFFER_SIZE);
		buf.order(ByteOrder.LITTLE_ENDIAN);
	} else {
		buf = readBuffer;
	}

	// 为溢出的数据创建一个 reader
	currentChannel.position(0L);
	SpilledBufferOrEventSequence seq =
			new SpilledBufferOrEventSequence(currentSpillFile, currentChannel, buf, pageSize);

	// spill 之后，新开一个 spill 文件
	createSpillingChannel();

	bytesWritten = 0L;
	return seq;
}
```

#### SpilledBufferOrEventSequence

直接来看最重要的 getNext 方法，从 fileChannel 中读取数据到 buffer 中，首先读取 9 字节的 header，然后根据 isBuffer 还原 Buffer 和 Event，返回给调用方

```java
public BufferOrEvent getNext() throws IOException {
	if (buffer.remaining() < HEADER_LENGTH) {
		buffer.compact();

		while (buffer.position() < HEADER_LENGTH) {
			// 从 fileChannel 中读取数据到 buffer 中
			if (fileChannel.read(buffer) == -1) {
				if (buffer.position() == 0) {
					// no trailing data
					// 没有数据
					return null;
				} else {
					throw new IOException("Found trailing incomplete buffer or event");
				}
			}
		}

		buffer.flip();
	}

	final int channel = buffer.getInt();
	final int length = buffer.getInt();
	final boolean isBuffer = buffer.get() == 0;

	if (isBuffer) {
		// deserialize buffer
		// 反序列化 buffer
		if (length > pageSize) {
			throw new IOException(String.format(
					"Spilled buffer (%d bytes) is larger than page size of (%d bytes)", length, pageSize));
		}

		MemorySegment seg = MemorySegmentFactory.allocateUnpooledSegment(pageSize);

		int segPos = 0;
		int bytesRemaining = length;
		
		// 循环读取，将 content 从 buffer 中读取到内存段中
		while (true) {
			int toCopy = Math.min(buffer.remaining(), bytesRemaining);
			if (toCopy > 0) {
				seg.put(segPos, buffer, toCopy);
				segPos += toCopy;
				bytesRemaining -= toCopy;
			}

			if (bytesRemaining == 0) {
				break;
			}
			else {
				buffer.clear();
				if (fileChannel.read(buffer) == -1) {
					throw new IOException("Found trailing incomplete buffer");
				}
				buffer.flip();
			}
		}

		Buffer buf = new NetworkBuffer(seg, FreeingBufferRecycler.INSTANCE);
		buf.setSize(length);

		return new BufferOrEvent(buf, channel);
	}
	else {
		// 反序列化事件
		if (length > buffer.capacity() - HEADER_LENGTH) {
			throw new IOException("Event is too large");
		}

		if (buffer.remaining() < length) {
			buffer.compact();

			while (buffer.position() < length) {
				if (fileChannel.read(buffer) == -1) {
					throw new IOException("Found trailing incomplete event");
				}
			}

			buffer.flip();
		}

		int oldLimit = buffer.limit();
		buffer.limit(buffer.position() + length);
		AbstractEvent evt = EventSerializer.fromSerializedEvent(buffer, getClass().getClassLoader());
		buffer.limit(oldLimit);

		return new BufferOrEvent(evt, channel);
	}
}
```

### CachedBufferBlocker

CachedBufferBlocker 用内存队列存储元素

#### 属性和构造函数

CachedBufferBlocker 直接用 ArrayDeque 存储元素

```java
// 页面大小，用于估计总缓存数据大小
private final int pageSize;

// 自上次翻转以来缓存的字节数
private long bytesBlocked;

// 用于缓存缓冲区或事件的当前内存队列
private ArrayDeque<BufferOrEvent> currentBuffers;

/**
 * 创建一个新的 CachedBufferBlocker，缓存内存队列中的缓冲区或事件
 */
public CachedBufferBlocker(int pageSize) {
	this.pageSize = pageSize;
	this.currentBuffers = new ArrayDeque<BufferOrEvent>();
}
```

#### add 方法

add 方法将 BufferOrEvent 加入队列

```java
public void add(BufferOrEvent boe) {
	bytesBlocked += pageSize;

	currentBuffers.add(boe);
}
```

#### rollOverReusingResources 方法和 rollOverWithoutReusingResources 方法

由于使用内存队列缓存元素，因此 rollOverReusingResources 方法直接调用 rollOverWithoutReusingResources 方法，rollOverWithoutReusingResources 直接将当前队列传入 CachedBufferOrEventSequence，然后创建一个新的 ArrayDeque 实例

```java
public BufferOrEventSequence rollOverReusingResources() {
	return rollOverWithoutReusingResources();
}

// 反转，获取缓存的队列
@Override
public BufferOrEventSequence rollOverWithoutReusingResources() {
	if (bytesBlocked == 0) {
		return null;
	}

	CachedBufferOrEventSequence currentSequence = new CachedBufferOrEventSequence(currentBuffers, bytesBlocked);
	currentBuffers = new ArrayDeque<BufferOrEvent>();
	bytesBlocked = 0L;

	return currentSequence;
}
```

#### CachedBufferOrEventSequence

CachedBufferOrEventSequence 简单的过分了，getNext 直接调用 queuedBuffers 的 poll 方法

```java
public static class CachedBufferOrEventSequence implements BufferOrEventSequence {

	private final ArrayDeque<BufferOrEvent> queuedBuffers;

	private final long size;

	CachedBufferOrEventSequence(ArrayDeque<BufferOrEvent> buffers, long size) {
		this.queuedBuffers = buffers;
		this.size = size;
	}

	@Override
	public void open() {}

	@Override
	@Nullable
	public BufferOrEvent getNext() {
		return queuedBuffers.poll();
	}

	@Override
	public void cleanup() {
		BufferOrEvent boe;
		while ((boe = queuedBuffers.poll()) != null) {
			if (boe.isBuffer()) {
				boe.getBuffer().recycleBuffer();
			}
		}
	}

	@Override
	public long size() {
		return size;
	}
}
```

在了解完 BarrierBuffer 中非常重要的 BufferOrEventSequence 之后，我们来看看 BarrierBuffer 的源码

BarrierBuffer 比 BarrierTracker 复杂了许多，实现上也大有不同，BarrierTracker 能够在队列中存储多个检查点，而 BarrierBuffer 仅仅**存储一个检查点**

### BarrierBuffer 的属性

```java
// input gate，从中接收网络 io 的元素
private final InputGate inputGate;

// 指示通道当前是否被阻塞/缓冲的标志
private final boolean[] blockedChannels;

// channel 的数量，一旦检查点收到了这么多 barriers，检查点被认为完成
private final int totalNumberOfInputChannels;

// 用于缓存 Buffer 和 Event，BufferSpiller or CachedBufferBlocker
private final BufferBlocker bufferBlocker;

// pending 的 Buffer/Event 序列，CachedBufferOrEventSequence or SpilledBufferOrEventSequence，必须在从 input gate 请求更多数据之前消费
private final ArrayDeque<BufferOrEventSequence> queuedBuffered;

// BarrierBuffer 中一共能缓存多少字节的数据
private final long maxBufferedBytes;

// 当前正在消费的序列
private BufferOrEventSequence currentBuffered;

// 当前处理的检查点 id
private long currentCheckpointId = -1L;

// 当前检查点已经接收了多少个 barrier
private int numBarriersReceived;

// 当前已经关闭的 channel 的数目
private int numClosedChannels;

// 缓存序列中一共有多少字节，用于和 maxBufferedBytes 比对，判断是否对齐失败
private long numQueuedBytes;

// 当前检查点开始的时间
private long startOfAlignmentTimestamp;

// 上一次检查点花费的时间
private long latestAlignmentDurationNanos;

// 表明已经 inputGate 中已经没有数据了
private boolean endOfStream;
```

### getNextNonBlocked 方法

```java
public BufferOrEvent getNextNonBlocked() throws Exception {
	while (true) {
		// 在获取新的缓冲序列之前，处理当前的
		Optional<BufferOrEvent> next;
		// 如果 currentBuffered 为空，则可以从 input gate 去请求新的数据
		if (currentBuffered == null) {
			next = inputGate.getNextBufferOrEvent();
		}
		// 否则，需要先处理 currentBuffered
		else {
			next = Optional.ofNullable(currentBuffered.getNext());
			// next 为 null 的话，完成本 BufferOrEventSequence，调用 completeBufferedSequence 去队列中获取下一个 BufferOrEventSequence
			if (!next.isPresent()) {
				completeBufferedSequence();
				return getNextNonBlocked();
			}
		}
		
		// input gate 输入为 null
		if (!next.isPresent()) {
			if (!endOfStream) {
				// 输入流结束，流继续缓冲数据
				endOfStream = true;
				releaseBlocksAndResetBarriers();
				return getNextNonBlocked();
			}
			else {
				// final end of both input and buffered data
				// 输入和缓冲数据的最终结束
				return null;
			}
		}

		BufferOrEvent bufferOrEvent = next.get();
		if (isBlocked(bufferOrEvent.getChannelIndex())) {
			// 如果当前 channel 被阻塞了，我们缓存 BufferOrEvent
			bufferBlocker.add(bufferOrEvent);
			// 调用 checkSizeLimit 判断当前缓存的字节是否超出 maxBufferedBytes
			checkSizeLimit();
		}
		else if (bufferOrEvent.isBuffer()) {
			return bufferOrEvent;
		}
		else if (bufferOrEvent.getEvent().getClass() == CheckpointBarrier.class) {
			if (!endOfStream) {
				// 只有在检查点有可能完成时才会处理障碍
				processBarrier((CheckpointBarrier) bufferOrEvent.getEvent(), bufferOrEvent.getChannelIndex());
			}
		}
		else if (bufferOrEvent.getEvent().getClass() == CancelCheckpointMarker.class) {
			processCancellationBarrier((CancelCheckpointMarker) bufferOrEvent.getEvent());
		}
		else {
			// EndOfPartitionEvent 标志着一个子分区完全被消费完毕
			if (bufferOrEvent.getEvent().getClass() == EndOfPartitionEvent.class) {
				processEndOfPartition();
			}
			return bufferOrEvent;
		}
	}
}
```

### releaseBlocksAndResetBarriers

releaseBlocksAndResetBarriers 方法会重置 blockedChannels，如果当前 currentBuffered 为 null 的时候，会调用 `bufferBlocker.rollOverReusingResources` 从 BufferSpiller/CachedBufferBlocker 中获取新的队列，由于当前 currentBuffered 为 null，资源可以重用，使用 rollOverReusingResources，如果 currentBuffered 不为 null，我们将当前的 currentBuffered 放回等待队列头部，然后获取新的 BufferOrEventSequence

```java
private void releaseBlocksAndResetBarriers() throws IOException {
	for (int i = 0; i < blockedChannels.length; i++) {
		blockedChannels[i] = false;
	}

	if (currentBuffered == null) {
		// 常见情况：没有更多缓冲数据
		currentBuffered = bufferBlocker.rollOverReusingResources();
		if (currentBuffered != null) {
			currentBuffered.open();
		}
	}
	else {
		// 不常见的情况：缓冲数据 pending
		// 如果我们有 pending 数据的话，将其推回
		// 因为我们没有完全消耗前面的序列，所以我们需要为这个序列分配一个新的缓冲区
		BufferOrEventSequence bufferedNow = bufferBlocker.rollOverWithoutReusingResources();
		if (bufferedNow != null) {
			bufferedNow.open();
			queuedBuffered.addFirst(currentBuffered);  // 这里是将当前的 currentBuffered 放入队列
			numQueuedBytes += currentBuffered.size();
			currentBuffered = bufferedNow;  // 然后设置 currentBuffered 为 bufferedNow
		}
	}


	// 下一个到来的障碍必须被假设是第一个
	numBarriersReceived = 0;

	if (startOfAlignmentTimestamp > 0) {
		latestAlignmentDurationNanos = System.nanoTime() - startOfAlignmentTimestamp;
		startOfAlignmentTimestamp = 0;
	}
}
```

### processBarrier

processBarrier 用于处理到来的 barrier，注释非常详细，就不解释了

```java
private void processBarrier(CheckpointBarrier receivedBarrier, int channelIndex) throws Exception {
	final long barrierId = receivedBarrier.getId();

	// 单输入 channel 的快捷通道
	if (totalNumberOfInputChannels == 1) {
		if (barrierId > currentCheckpointId) {
			currentCheckpointId = barrierId;
			notifyCheckpoint(receivedBarrier);
		}
		return;
	}

	// 多输入 channel 的通用代码
	if (numBarriersReceived > 0) {
		// 只有在某些对齐已经进行并且未取消时才会出现这种情况
		if (barrierId == currentCheckpointId) {
			// 常规 case
			onBarrier(channelIndex);
		}
		else if (barrierId > currentCheckpointId) {
			// 我们没有在另一个检查点开始之前，完成当前的检查点
			// 让任务知道我们没有完成当前检查点
			notifyAbort(currentCheckpointId, new CheckpointDeclineSubsumedException(barrierId));

			// 停止当前的检查点，这里要开始一个新的检查点，所以 releaseBlocksAndResetBarriers 新
			// 生成一个 BufferOrEventSequence，将老的返回队列，因为这样更有机会能访问到新检查点的 barrier
			releaseBlocksAndResetBarriers();

			// 开始一个新的检查点
			beginNewAlignment(barrierId, channelIndex);
		}
		else {
			// 忽略早期检查点的尾随障碍（现在已过时）
			return;
		}
	}
	else if (barrierId > currentCheckpointId) {
		// 新的检查点的第一个 barrier
		beginNewAlignment(barrierId, channelIndex);
	}
	else {
		// 要么当前检查点被取消了（numBarriers == 0）
		// 要么此屏障来自旧的包含检查点
		return;
	}

	// 检查我们是否有所有的障碍 - 因为取消的检查点始终没有障碍，这只能在未取消的检查点上发生
	if (numBarriersReceived + numClosedChannels == totalNumberOfInputChannels) {
		// 触发检查点
		releaseBlocksAndResetBarriers();
		notifyCheckpoint(receivedBarrier);
	}
}
```

### processCancellationBarrier

processCancellationBarrier 用于处理检查点取消的事件，当 numBarriersReceived 大于 0 的时候，如果 `barrierId == currentCheckpointId`，直接取消当前的检查点，如果 `barrierId > currentCheckpointId`，将两个检查点都取消，如果 numBarriersReceived 等于 0 的时候，说明没有接收到 currentCheckpointId 的 barrier，直接将 currentCheckpointId 赋为 barrierId，取消 barrierId 的检查点即可，之前 currentCheckpointId 的事件来了都会被抛弃

```java
private void processCancellationBarrier(CancelCheckpointMarker cancelBarrier) throws Exception {
	final long barrierId = cancelBarrier.getCheckpointId();

	// 依旧是单输入 channel 的快捷通道
	if (totalNumberOfInputChannels == 1) {
		if (barrierId > currentCheckpointId) {
			currentCheckpointId = barrierId;
			notifyAbortOnCancellationBarrier(barrierId);
		}
		return;
	}

	// 多输入 channel 的通用代码
	if (numBarriersReceived > 0) {
		// 只有在某些对齐正在进行且没有取消任何内容时才会出现这种情况
		if (barrierId == currentCheckpointId) {

			releaseBlocksAndResetBarriers();
			notifyAbortOnCancellationBarrier(barrierId);
		}
		else if (barrierId > currentCheckpointId) {
			// 我们取消了之后的检查点，自然也取消当前的

			// 这会停止当前对齐
			releaseBlocksAndResetBarriers();

			currentCheckpointId = barrierId;
			startOfAlignmentTimestamp = 0L;
			latestAlignmentDurationNanos = 0L;

			notifyAbort(currentCheckpointId, new CheckpointDeclineSubsumedException(barrierId));

			notifyAbortOnCancellationBarrier(barrierId);
		}

	}
	else if (barrierId > currentCheckpointId) {
		// 新检查点的第一道屏障直接取消

		currentCheckpointId = barrierId;

		startOfAlignmentTimestamp = 0L;
		latestAlignmentDurationNanos = 0L;

		notifyAbortOnCancellationBarrier(barrierId);
	}

}
```

## 总结

今天我们给大家介绍了 CheckpointBarrierHandler，这在检查点中非常重要，希望对大家有帮助