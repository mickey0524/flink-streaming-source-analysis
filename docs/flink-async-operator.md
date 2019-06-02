# flink 中的异步操作符

前面我们介绍的所有操作符都是同步的，当流元素到达操作符，执行 ProcessElement 方法，将处理的结果 emit 到下一个操作符，这篇文章我们来讲一下 flink 中的异步操作符

我们先简要介绍一下 flink 是如何异步操作流元素的，当流元素到达异步操作符，我们将流元素暂时存储到一个队列中，当用户针对这个流元素执行完某种异步操作，得到结果之后，执行 asyncInvoke 方法通知异步操作符，异步操作符再将结果 emit 给下游操作符

## 一个小栗子

```java
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
Integer[] integers = new Integer[]{1, 2, 3, 4};

DataStream<Integer> dataStream = env.fromElements(integers);

AsyncDataStream.orderedWait(dataStream, new AsyncFunction<Integer, Integer>() {
    @Override
    public void asyncInvoke(Integer input, ResultFuture<Integer> resultFuture) throws Exception {
        Thread.sleep(ThreadLocalRandom.current().nextInt(5));
        ArrayList<Integer> l = new ArrayList<>();
        l.add(input);
        resultFuture.complete(l);
    }
}, 10, TimeUnit.MILLISECONDS).printToErr();
```

👆的栗子非常简单，从本地集合生成流，然后调用 AsyncDataStream 的 ordererWait 方法接收 AsyncFunction，进而生成 AsyncWaitOperator，我们在 asyncInvoke 中随机等待一段时间，然后通知异步操作符操作完成，可以将元素输出到错误流了

我们一口气说了很多名词，很多童鞋可能一头雾水，没有关系，我们下面一个一个类来讲解

## AsyncDataStream

AsyncDataStream 其实可以说是一个工具类，用于给 DataStream 加上异步操作符，AsyncDataStream 提供两个方法 —— orderedWait 和 unorderedWait，orderedWait 能够让流元素流出操作符的顺序和流入操作符的顺序完全相同，unorderedWait 保证 Watermark 和 StreamRecord 的相对顺序一致，但是两个 Watermark 之间的 StreamRecord 可以乱序输出

```java
public class AsyncDataStream {
	private static <IN, OUT> SingleOutputStreamOperator<OUT> addOperator(
			DataStream<IN> in,
			AsyncFunction<IN, OUT> func,
			long timeout,
			int bufSize,
			OutputMode mode) {

		AsyncWaitOperator<IN, OUT> operator = new AsyncWaitOperator<>(
			in.getExecutionEnvironment().clean(func),
			timeout,
			bufSize,
			mode);
		
		return in.transform("async wait operator", outTypeInfo, operator);
	}

	public static <IN, OUT> SingleOutputStreamOperator<OUT> unorderedWait(
			DataStream<IN> in,
			AsyncFunction<IN, OUT> func,
			long timeout,
			TimeUnit timeUnit,
			int capacity) {
		return addOperator(in, func, timeUnit.toMillis(timeout), capacity, OutputMode.UNORDERED);
	}

	public static <IN, OUT> SingleOutputStreamOperator<OUT> orderedWait(
			DataStream<IN> in,
			AsyncFunction<IN, OUT> func,
			long timeout,
			TimeUnit timeUnit,
			int capacity) {
		return addOperator(in, func, timeUnit.toMillis(timeout), capacity, OutputMode.ORDERED);
	}
}
```

从源码中可以看到，orderedWait 方法和 unorderedWait 方法都调用了 addOperator 方法，addOperator 用于生成 AsyncWaitOperator，然后调用输入流的 transform 方法生成 StreamTransformation 并加入 Transformation 树用于生成 StreamGraph

我们再来看一下 orderedWait 和 unorderedWait 的参数

* DataStream<IN> in: 输入流，也就是添加异步操作符的目标流
* AsyncFunction<IN, OUT> func: 用户定义的异步函数，主要用于异步操作完毕后通知操作符，以及在异步操作超时的时候抛出异常
* long timeout: 异步操作处理时间的 ttl
* TimeUnit timeUnit: 异步操作处理时间的单位
* int capacity: 缓存流元素的队列的大小，默认为 100

## AsyncFunction

👇是 AsyncFunction 的代码，可以看到 AsyncFunction 接口定义了两个函数，asyncInvoke 函数用于在异步操作完毕的时候通知操作符（例如访问 MySQL 完成了），调用 ResultFuture 的 complete 即可，timeout 函数用于 asyncInvoke 函数执行时间超过 orderedWait 和 unorderedWait 中
timeout 参数的时候抛出异常

```java
public interface AsyncFunction<IN, OUT> extends Function, Serializable {
	/**
	 * 触发每个流输入的异步操作
	 */
	void asyncInvoke(IN input, ResultFuture<OUT> resultFuture) throws Exception;

	/**
	 * asyncInvoke 操作 timeout 了，默认抛出异常
	 */
	default void timeout(IN input, ResultFuture<OUT> resultFuture) throws Exception {
		resultFuture.completeExceptionally(
			new TimeoutException("Async function call has timed out."));
	}
}
```

## StreamElementQueue

异步操作符需要将流入的流元素暂存到 StreamElementQueue 中，当异步操作完成（asyncInvoke 调用 resultFuture.complete），再将队列中缓存的流元素 emit 到下游，StreamElementQueue 的源码如下所示，定义了七个方法（类似消息队列的 API），OrderedStreamElementQueue 和 UnorderedStreamElementQueue 是 StreamElementQueue 的两个实现类

```java
public interface StreamElementQueue {
	/**
	 * 将 streamElementQueueEntry 参数加入队列，如果队列满了，则阻塞直到队列有空余
	 */
	<T> void put(StreamElementQueueEntry<T> streamElementQueueEntry) throws InterruptedException;

	/**
	 * 尝试将 streamElementQueueEntry 加入队列，加入成功返回 true，失败返回 false
	 */
	<T> boolean tryPut(StreamElementQueueEntry<T> streamElementQueueEntry) throws InterruptedException;

	/**
	 * 查看队列的头部并返回第一个完成的 AsyncResult
	 * 此操作是阻塞操作，只有在找到完成的异步结果后才会返回
	 */
	AsyncResult peekBlockingly() throws InterruptedException;

	/**
	 * 从该队列的头部找到第一个完成的 AsyncResult，并 remove
	 * 此操作是阻塞的，只有在找到完成的异步结果后才会返回
	 */
	AsyncResult poll() throws InterruptedException;

	/**
	 * 返回当前队列中包含的 StreamElementQueueEntry 的集合
	 */
	Collection<StreamElementQueueEntry<?>> values() throws InterruptedException;

	/**
	 * 返回队列是否是空的
	 */
	boolean isEmpty();

	/**
	 * 返回队列的大小
	 */
	int size();
}
```

## OrderedStreamElementQueue

OrderedStreamElementQueue 对应 AsyncDataStream 中的 orderedWait 方法，OrderedStreamElementQueue 按照 StreamElementQueueEntry（StreamRecord 或 Watermark） 被加入队列的顺序 emit 异步结果，因此，即使完成顺序是任意的，队列的输出顺序还是严格按照插入的顺序来

首先来看看 OrderedStreamElementQueue 中的属性和构造函数，容易发现，OrderedStreamElementQueue 内部存放 StreamElementQueueEntry 的载体是一个容量为 capacity 的 ArrayDeque，构造函数中初始化了一个 ReentrantLock，lock 创建了两个条件，notFull 用于通知 put 方法当前队列有空余位置，可以插入新的元素，headIsCompleted 用于通知 peekBlockingly 方法和 poll 方法队列首部元素异步执行完毕，可以消费了

```java
// 队列的容量
private final int capacity;

// 执行 onCompletion 回调的 Executor
private final Executor executor;

// 锁和条件，用于阻塞队列
private final ReentrantLock lock;
private final Condition notFull;
private final Condition headIsCompleted;

// 用于插入 StreamElementQueueEntries 的队列
private final ArrayDeque<StreamElementQueueEntry<?>> queue;

public OrderedStreamElementQueue(
		int capacity,
		Executor executor,
		OperatorActions operatorActions) {

	this.capacity = capacity;

	this.executor = Preconditions.checkNotNull(executor, "executor");

	this.lock = new ReentrantLock(false);
	this.headIsCompleted = lock.newCondition();
	this.notFull = lock.newCondition();

	this.queue = new ArrayDeque<>(capacity);
}
```

再来看看 peekBlockingly 和 poll 方法（实现 StreamElementQueue 定义的方法），peekBlockingly 获取位于队列首部且异步执行完成的元素，poll 获取并**删除**位于队列首部且异步执行完成的元素，当队列为空或者队列首部元素没有完成异步操作，这两个方法将通过调用 `headIsCompleted.await` 阻塞，此外，poll 方法由于会删除元素，因此会调用 `notFull.signAll` 来通知 put 方法现在队列有位置了

```java
/**
 * 获取队列首部的元素，如果队列为空，或者队列首部的元素没有执行完，阻塞
 */
@Override
public AsyncResult peekBlockingly() throws InterruptedException {
	lock.lockInterruptibly();

	try {
		while (queue.isEmpty() || !queue.peek().isDone()) {
			headIsCompleted.await();
		}

		return queue.peek();
	} finally {
		lock.unlock();
	}
}

/**
 * 获取并删除队列首部的元素，如果队列为空，或者队列首部的元素没有执行完，阻塞
 */
@Override
public AsyncResult poll() throws InterruptedException {
	lock.lockInterruptibly();

	try {
		while (queue.isEmpty() || !queue.peek().isDone()) {
			headIsCompleted.await();
		}

		// 唤醒 notFull 条件阻塞的 put 方法
		notFull.signalAll();

		return queue.poll();
	} finally {
		lock.unlock();
	}
}
```

接下来，我们看看 put 和 tryPut 方法（实现 StreamElementQueue 定义的方法），这两个方法都是用于往队列中添加元素的，区别是，put 方法在队列没有空位的时候，会调用 `notFull.await` 等待 poll 方法执行 `notFull.signAll`，而 tryPut 方法在队列没有空位的时候不会阻塞等待，直接返回 false

```java
// 插入一个 StreamElementQueueEntry，如果队列满，阻塞
@Override
public <T> void put(StreamElementQueueEntry<T> streamElementQueueEntry) throws InterruptedException {
	lock.lockInterruptibly();

	try {
		while (queue.size() >= capacity) {
			notFull.await();
		}

		addEntry(streamElementQueueEntry);
	} finally {
		lock.unlock();
	}
}

// 插入一个 StreamElementQueueEntry，如果队列满，返回 false
@Override
public <T> boolean tryPut(StreamElementQueueEntry<T> streamElementQueueEntry) throws InterruptedException {
	lock.lockInterruptibly();

	try {
		if (queue.size() < capacity) {
			addEntry(streamElementQueueEntry);

			return true;
		} else {
			return false;
		}
	} finally {
		lock.unlock();
	}
}

/**
 * 将 StreamElementQueueEntry 加入队列
 * 并且注册一个 entry 完成时候调用的回调函数
 */
private <T> void addEntry(StreamElementQueueEntry<T> streamElementQueueEntry) {
	assert(lock.isHeldByCurrentThread());

	queue.addLast(streamElementQueueEntry);

	streamElementQueueEntry.onComplete(
		(StreamElementQueueEntry<T> value) -> {
			onCompleteHandler(value);
		},
		executor);
}

/**
 * 检查当前队列的首部元素是否执行完毕
 */
private void onCompleteHandler(StreamElementQueueEntry<?> streamElementQueueEntry) throws InterruptedException {
	lock.lockInterruptibly();

	try {
		// 异步执行完成，通知 poll 和 peekBlockingly 函数
		if (!queue.isEmpty() && queue.peek().isDone()) {
			headIsCompleted.signalAll();
		}
	} finally {
		lock.unlock();
	}
}
```

我们可以看到 put 方法和 tryPut 方法内部都掉用了 addEntry 方法，addEntry 方法用于将元素加入队列的末端，同时注册一个回调函数，当元素异步执行完毕的时候，检查当前位于**队列首部**的元素是否执行完毕（通过这种方式保证流入的顺序和流出的顺序完全一致），如果 `queue.peek().isDone()` 返回 true，调用 `headIsCompleted.signalAll()` 通知 peekBlockingly 和 poll 方法，可能有同学第一眼（包括我自己）会认为 onCompleteHandler 中的 if 应该换成 while，不然有可能会丢数据，其实不然，signalAll 会触发所有的等待者，而且线程调用 peekBlockingly 和 poll 的时候，都会先判断，条件不符合再等待

StreamElementQueue 剩余的 values、isEmpty 和 size 方法比较简单，这里就不介绍了，感兴趣的同学可以去 `org.apache.flink.streaming.api.operators.async.queue.OrderedStreamElementQueue.java` 中自行查看

## UnorderedStreamElementQueue

UnorderedStreamElementQueue 对应 AsyncDataStream 中的 unorderedWait 方法，元素在异步操作完成之后就可以被 emit，UnorderedStreamElementQueue 保证水印和流元素顺序的相对一致，举个例子，用 w 指代水印，用 r 指代流元素，输入顺序是 `w1，r1，r2，r3，w2，r4`，w1 和 w2 之间的流元素 r1，r2，r3 可以乱序 emit，但是整体一定是以 w1 -> (r1，r2，r3 的某种排列) -> w2 的顺序 emit 的，同理，即使 r4 已经异步执行完毕，还是需要等待 w2 emit 了，r4 才能 emit

首先来看看 UnorderedStreamElementQueue 中的属性和构造函数，与 OrderedStreamElementQueue 中仅仅用一个 ArrayDeque 不同，UnorderedStreamElementQueue 的实现复杂了许多，completedQueue 用来保存已完成异步操作的元素，poll 方法和 peekBlockingly 方法消费的就是 completedQueue，uncompletedQueue 是用来保证水印和流元素顺序的相对一致，在 uncompletedQueue 中，流元素队列被水印间隔开，firstSet 是第一个（按时间顺序排列最早的）未完成的流元素队列条目集，lastSet 是最后一个（按时间顺序排列最晚）未完成的流元素队列条目集，最开始 firstSet 和 lastSet 指向的是同一个内存地址，这几个队列/集合如何使用可以看看下面的 addEntry 方法

```java
// 队列的容量
private final int capacity;

// 执行 onComplete 回调的 Executor
private final Executor executor;

// 由水印分段的未完成的流元素队列条目的队列
private final ArrayDeque<Set<StreamElementQueueEntry<?>>> uncompletedQueue;

// 已完成的流元素队列条目的队列
private final ArrayDeque<StreamElementQueueEntry<?>> completedQueue;

// 第一个（按时间顺序排列最早的）未完成的流元素队列条目集
private Set<StreamElementQueueEntry<?>> firstSet;

// 最后（按时间顺序排列最晚）未完成的流元素队列条目集
// 新的流元素队列条目将插入此集合中
private Set<StreamElementQueueEntry<?>> lastSet;
private volatile int numberEntries;

// 锁和条件，用于阻塞队列
private final ReentrantLock lock;
private final Condition notFull;
private final Condition hasCompletedEntries;

public UnorderedStreamElementQueue(
		int capacity,
		Executor executor,
		OperatorActions operatorActions) {

	this.capacity = capacity;

	this.executor = Preconditions.checkNotNull(executor, "executor");

	this.uncompletedQueue = new ArrayDeque<>(capacity);
	this.completedQueue = new ArrayDeque<>(capacity);

	// 最开始的时候，firstSet 和 lastSet 指向的是同一块内存地址
	this.firstSet = new HashSet<>(capacity);
	this.lastSet = firstSet;

	this.numberEntries = 0;

	this.lock = new ReentrantLock();
	this.notFull = lock.newCondition();
	this.hasCompletedEntries = lock.newCondition();
}
```

OrderedStreamElementQueue 中的七个方法和 UnorderedStreamElementQueue 中类型，这里就不一一介绍了，感兴趣的同学可以去 `org.apache.flink.streaming.api.operators.async.queue.UnorderedStreamElementQueue.java` 中查看，我们这里重点讲一下 addEntry 方法，这个方法的实现和 OrderedStreamElementQueue 中天差地别

```java
/**
 * 如果给定的流元素队列条目不是水印，则将其添加到当前的最后一个集合
 * 如果它是水印，则停止添加到当前的最后一组，将水印插入其自己的集合中并添加新的最后一组
 */
private <T> void addEntry(StreamElementQueueEntry<T> streamElementQueueEntry) {
	assert(lock.isHeldByCurrentThread());

	if (streamElementQueueEntry.isWatermark()) {
		lastSet = new HashSet<>(capacity);

		if (firstSet.isEmpty()) {
			firstSet.add(streamElementQueueEntry);
		} else {
			Set<StreamElementQueueEntry<?>> watermarkSet = new HashSet<>(1);
			watermarkSet.add(streamElementQueueEntry);
			uncompletedQueue.offer(watermarkSet);
		}
		uncompletedQueue.offer(lastSet);  // uncompletedQueue 插入新的 lastSet，随后来的 StreamElement 都会直接写入 uncompletedQueue
	} else {
		lastSet.add(streamElementQueueEntry);
	}

	streamElementQueueEntry.onComplete(
		(StreamElementQueueEntry<T> value) -> {
			onCompleteHandler(value);
		},
		executor);

	numberEntries++;
}

/**
 * 回调给定流元素队列条目的 onComplete 事件
 * 每当队列条目完成时，检查该条目是否属于第一组，如果是这种情况，则将元素添加到已完成的条目队列中，从中可以使用该元素
 * 如果第一个集合变为空，则从未完成的条目队列中轮询下一个集
 * 然后，将来自此新集的已完成条目添加到已完成的条目队列中
 */
public void onCompleteHandler(StreamElementQueueEntry<?> streamElementQueueEntry) throws InterruptedException {
	lock.lockInterruptibly();

	try {
		if (firstSet.remove(streamElementQueueEntry)) {
			completedQueue.offer(streamElementQueueEntry);
			// 当从 firstSet 中 remove 一个元素，然后 firstSet 变为空
			// 说明可以访问 uncompletedQueue 中下一个 set 了
			// 通过 while 一直循环下去，
			// 当 firstSet 重新指向 lastSet 的时候，跳出循环
			// 或者是当前 firstSet.isEmpty() == false，这意味着目前 firstSet
			// 中还有没有完成异步操作的元素，需要等待
			while (firstSet.isEmpty() && firstSet != lastSet) {
				firstSet = uncompletedQueue.poll();

				Iterator<StreamElementQueueEntry<?>> it = firstSet.iterator();

				while (it.hasNext()) {
					StreamElementQueueEntry<?> bufferEntry = it.next();

					if (bufferEntry.isDone()) {
						completedQueue.offer(bufferEntry);
						it.remove();
					}
				}
			}

			hasCompletedEntries.signalAll();
		}
	} finally {
		lock.unlock();
	}
}
```

我们来讲解一下，当 addEntry 的参数是流元素的时候，直接将元素加入 lastSet，如果到来的是水印，则说明需要分组了，因为需要保证水印和流元素流出的顺序相对一致，因此流元素需要被水印间隔开，因此给 lastSet 重新赋值，有同学可能会问，流元素是加入 lastSet 的，这里又调用 `lastSet = new HashSet<>(capacity)`，那么之前保存的流元素不就都丢了吗，其实不是这样的，因为构造函数中 `lastSet = firstSet`，之前保存的元素可以通过 firstSet 访问（onCompleteHandler 也是这么做的），接着，判断当前 firstSet 是否为空，如果为空说明当前队列中没有元素，直接写入 firstSet 即可，否则，同样需要创建一个 HashSet，然后将水印放进去，最后将包裹水印的 HashSet 和新创建的 lastSet 依次加入 uncompletedQueue，这样在 uncompletedQueue 中水印就分隔了不同的流元素集合

onCompleteHandler 实现也非常巧妙，从 firstSet 中移除完成异步操作的流元素并将其直接加入 completedQueue，这会使得流元素变得无序，当目前的 firstSet 为空的时候，说明可以访问 uncompletedQueue 中下一个 set 了
通过 while 一直循环下去，当 firstSet 重新指向 lastSet 的时候，跳出循环（重新将 firstSet 和 lastSet 指向同一块内存区域），或者是当前 firstSet.isEmpty() == false，这意味着目前 firstSet 中还有没有完成异步操作的元素，需要等待

## StreamElementQueueEntry

StreamElementQueueEntry 是 StreamElementQueue 中存储的元素，StreamElementQueueEntry 可能包裹一个 Watermark，也可能包裹一个 StreamRecord，分别对应 WatermarkQueueEntry 和 StreamRecordQueueEntry，下面来看看 StreamElementQueueEntry 的源码，
getFuture 方法返回一个 CompletableFuture，包裹元素的异步操作，onComplete 方法提供回调函数的注册接口，当 CompletableFuture 完成的时候，注册的 completeFunction 会被调用

```java
/**
 * StreamElementQueue 的实体类
 * 流元素队列实体存储 StreamElement 
 * 此外，当队列实体完成的时候允许注册回调
 */
@Internal
public abstract class StreamElementQueueEntry<T> implements AsyncResult {

	/**
	 * 如果流元素队列实体完成，返回 true，否则，返回 false
	 */
	public boolean isDone() {
		return getFuture().isDone();
	}

	/**
	 * 当本队列条目完成的时候，注册 completeFunction
	 */
	public void onComplete(
			final Consumer<StreamElementQueueEntry<T>> completeFunction,
			Executor executor) {
		final StreamElementQueueEntry<T> thisReference = this;

		getFuture().whenCompleteAsync(
			// call the complete function for normal completion as well as exceptional completion
			// see FLINK-6435
			(value, throwable) -> completeFunction.accept(thisReference),
			executor);
	}

	protected abstract CompletableFuture<T> getFuture();
}
```

## Emitter

当队列中的元素异步操作完毕，自然需要一个消费者，否则，队列会一直积压数据，Emitter 就是这个消费者，用于消费队列中的元素并将它们输出到给定的下游操作符，直接来看源码

Emitter 实现了 Runnable 接口，会作为一个常驻线程在 AsyncWaitOperator
中使用，run 方法中调用 `streamElementQueue.peekBlockingly()` 方法获取异步操作完毕的元素，这里这里使用的是 peekBlockingly 而不是 poll，因为有检查点的存在，只有真正 emit 到下游之后，才会调用 poll 方法删除元素，避免丢失元素，output 方法判断 asyncResult 是水印还是流元素，然后 emit 到下游操作符，`checkpointLock.notifyAll()` 会通知 AsyncWaitOperator 我们消费了 streamElementQueue，可以继续往队列中写元素了

```java
	public void run() {
		while (running) {
			// 阻塞等待下一个队列中完成异步操作的元素
			AsyncResult streamElementEntry = streamElementQueue.peekBlockingly();

			output(streamElementEntry);
		}
	}

	// 输出已完成的异步操作
	private void output(AsyncResult asyncResult) throws InterruptedException {
		// 如果是 watermark 的话
		if (asyncResult.isWatermark()) {
			synchronized (checkpointLock) {
				// 将 asyncResult 转为 watermark
				AsyncWatermarkResult asyncWatermarkResult = asyncResult.asWatermark();

				// 输出 watermark
				output.emitWatermark(asyncWatermarkResult.getWatermark());

				// 从异步收集器缓冲区中删除 peeked 元素，以便不再检查
				streamElementQueue.poll();

				// 通知主线程异步收集器缓冲区中还剩余空间
				checkpointLock.notifyAll();
			}
		} else {
			// 将 asyncResult 转为结果集合
			AsyncCollectionResult<OUT> streamRecordResult = asyncResult.asResultCollection();

			if (streamRecordResult.hasTimestamp()) {
				timestampedCollector.setAbsoluteTimestamp(streamRecordResult.getTimestamp());
			} else {
				timestampedCollector.eraseTimestamp();
			}

			synchronized (checkpointLock) {
				Collection<OUT> resultCollection = streamRecordResult.get();
				// 结果集合中的 StreamRecord 时间戳相同
				if (resultCollection != null) {
					for (OUT result : resultCollection) {
						timestampedCollector.collect(result);
					}
				}

				streamElementQueue.poll();

				checkpointLock.notifyAll();
			}
		}
	}
}
```

## AsyncWaitOperator

AsyncWaitOperator 实现了异步操作符

* 创建 StreamElementQueue

	```java
	switch (outputMode) {
		case ORDERED:
			queue = new OrderedStreamElementQueue(
				capacity,
				executor,
				this);
			break;
		case UNORDERED:
			queue = new UnorderedStreamElementQueue(
				capacity,
				executor,
				this);
			break;
		default:
			throw new IllegalStateException("Unknown async mode: " + outputMode + '.');
	}
	```
	
* 创建 Emitter

	```java
	this.emitter = new Emitter<>(checkpointingLock, output, queue, this);

	// 开始 emitter 线程，emitter 实现了 Runnable 接口 
	this.emitterThread = new Thread(emitter, "AsyncIO-Emitter-Thread (" + getOperatorName() + ')');
	emitterThread.setDaemon(true);  // 设为常驻线程
	emitterThread.start();  // 启动 emitter 线程
	```

* 处理到来的流元素

	```java
	public void processElement(StreamRecord<IN> element) throws Exception {
		// 用 StreamRecordQueueEntry 包裹 StreamRecord
		final StreamRecordQueueEntry<OUT> streamRecordBufferEntry = new StreamRecordQueueEntry<>(element);

		if (timeout > 0L) {
			// 注册一个 timeoutTimestamp 的进程时间定时器
			long timeoutTimestamp = timeout + getProcessingTimeService().getCurrentProcessingTime();

			final ScheduledFuture<?> timerFuture = getProcessingTimeService().registerTimer(
				timeoutTimestamp,
				new ProcessingTimeCallback() {
					@Override
					public void onProcessingTime(long timestamp) throws Exception {
						// 用户函数处理 timeout
						userFunction.timeout(element.getValue(), streamRecordBufferEntry);
					}
				});

			// 我们完成了这个 StreamRecordQueueEntry，因此取消定时器
			// cancel 操作会取消设置的定时器
			streamRecordBufferEntry.onComplete(
				(StreamElementQueueEntry<Collection<OUT>> value) -> {
					timerFuture.cancel(true);
				},
				executor);
		}

		addAsyncBufferEntry(streamRecordBufferEntry);

		userFunction.asyncInvoke(element.getValue(), streamRecordBufferEntry);
	}
	
	private <T> void addAsyncBufferEntry(StreamElementQueueEntry<T> streamElementQueueEntry) throws InterruptedException {
		assert(Thread.holdsLock(checkpointingLock));

		while (!queue.tryPut(streamElementQueueEntry)) {
			// 我们等待 emitter 告诉我们队列中有空余位置
			// 这里用 put 方法感觉就可以。。put 方法里有 Reentrantlock 的 condition 控制
			checkpointingLock.wait();
		}

	}
	```
	
	用 StreamRecordQueueEntry 包裹 StreamRecord。这里有 timeout 的处理，当流元素到达 AsyncWaitOperator 的时候，我们根据设置的 timeout + 当前进程时间得到 expireTime，然后设置一个 expireTime 时间触发的定时器，用来调用 AsyncFunction 中的 timeout 报告超时，同时调用 `streamRecordBufferEntry.onComplete` 设置了一个回调，当异步操作完成的时候，取消定时器，我们调用 `addAsyncBufferEntry(streamRecordBufferEntry)` 将元素加入队列，最后调用 `userFunction.asyncInvoke` 等待用户调用 `streamRecordBufferEntry.complete` 通知异步操作完成
	
* 处理到来的水印

	使用 WatermarkQueueEntry 包裹 Watermark，然后将 WatermarkQueueEntry 加入队列

	```java
	public void processWatermark(Watermark mark) throws Exception {
		WatermarkQueueEntry watermarkBufferEntry = new WatermarkQueueEntry(mark);

		addAsyncBufferEntry(watermarkBufferEntry);
	}
	```

## 总结

这篇文章我们详细讲解了 flink 中 AsyncWaitOperator 是如何实现的，其实不外乎文章开头说的步骤，当流元素到来的时候，我们使用 StreamElementQueue 进行保存，等待异步操作的完成，用户调用 AsyncFunction 中的 asyncInvoke 方法通知队列当前流元素的异步操作完成了，队列判断元素是否能被消费，如果可以的话，将元素交给 emitter emit 到下游操作符