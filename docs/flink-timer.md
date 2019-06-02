# flink 中的定时器

这篇文章来讲一下 flink 中的定时器，顾名思义，定时器就是用户指定一个未来的时间，当时间到达的时候，会调用用户设置的回调函数。flink 中的进程时间定时器和事件时间定时器都依赖于底层的 ProcessingTimeService，flink 提供了 ProcessingTimeService 的默认实现 —— SystemProcessingTimeService，我们先来看看这个类

## SystemProcessingTimeService

SystemProcessingTimeService 继承了 ProcessingTimeService，它是一个 flink 提供的默认的进程时间服务，SystemProcessingTimeService 使用 `System.currentTimeMillis()` 的结果作为当前的进程时间，同时使用 ScheduledThreadPoolExecutor 来注册定时器，SystemProcessingTimeService 提供两种注册定时器的方法 —— registerTimer 和 scheduleAtFixedRate

* getCurrentProcessingTime

	getCurrentProcessingTime 方法返回当前的进程时间
	
	```java
	public long getCurrentProcessingTime() {
		return System.currentTimeMillis();
	}
	```	

* timerService
	
	timerService 是 SystemProcessingTimeService 的一个属性，用于注册定时器，SystemProcessingTimeService 实例话一个 ScheduledThreadPoolExecutor 来作为 timerService
	
	```java
	if (threadFactory == null) {
		this.timerService = new ScheduledThreadPoolExecutor(1);
	} else {
		this.timerService = new ScheduledThreadPoolExecutor(1, threadFactory);
	}
	```

* registerTimer

	registerTimer 方法接收一个未来的时间戳和一个回调函数，当时间到达给定的时间戳的时候，回调函数会被调用一次。从源码中可以看到，registerTimer 方法调用了 timerService 的 schedule 方法，传入了一个 TriggerTask 实例，TriggerTask 的 run 方法会在定时器触发的时候被调用，run 方法首先获取全局的锁，然后检查进程时间服务是否可用，可用的话调用 `target.onProcessingTime(timestamp)`，执行回调函数，这里注意一下，定时器有可能在给定的 timestamp 之后被触发（调度原因 + 获取 lock原因），但是 onProcessingTime 传入的参数是 registerTimer 方法传入的 timestamp 参数
	
	```java
	public ScheduledFuture<?> registerTimer(long timestamp, ProcessingTimeCallback target) {

		// 将定时器的触发延迟 1 ms 使得语义与水印对齐
		// 水印 T 表示我们将来不会看到时间戳小于或等于 T 的元素
		// 因此，在处理时间内，我们需要将定时器的延迟时间延迟一毫秒
		long delay = Math.max(timestamp - getCurrentProcessingTime(), 0) + 1;

		// 我们直接注册定时器
		return timerService.schedule(
					new TriggerTask(status, task, checkpointLock, target, timestamp), delay, TimeUnit.MILLISECONDS);
		}
	}

	private static final class TriggerTask implements Runnable {

		private final AtomicInteger serviceStatus;
		private final Object lock;
		private final ProcessingTimeCallback target;
		private final long timestamp;
		private final AsyncExceptionHandler exceptionHandler;

		private TriggerTask(
				final AtomicInteger serviceStatus,
				final AsyncExceptionHandler exceptionHandler,
				final Object lock,
				final ProcessingTimeCallback target,
				final long timestamp) {

			this.serviceStatus = Preconditions.checkNotNull(serviceStatus);
			this.exceptionHandler = Preconditions.checkNotNull(exceptionHandler);
			this.lock = Preconditions.checkNotNull(lock);
			this.target = Preconditions.checkNotNull(target);
			this.timestamp = timestamp;
		}

		@Override
		public void run() {
			// 当定时器触发的时候，首先获取全局的锁，然后检查进程时间服务是否可用
			// 可用的话调用 target.onProcessingTime(timestamp)，执行回调函数
			synchronized (lock) {
				if (serviceStatus.get() == STATUS_ALIVE) {
					target.onProcessingTime(timestamp);
				}
			}
		}
	}
	```

* scheduleAtFixedRate

	scheduleAtFixedRate 方法接收一个 initialDelay（初始延迟）、一个 period（周期数值）以及一个回调函数，回调函数会在 initialDelay 之后第一次被调用，然后以 period 周期性的调用，可以理解为回调函数会在 initialDelay -> initialDelay + period -> initialDelay + 2 * period -> ... 被调用。从源码中可以看到，scheduleAtFixedRate 方法调用了 timerService 的 scheduleAtFixedRate 方法，传入了一个 RepeatedTriggerTask 实例，RepeatedTriggerTask 的 run 方法会在定时器触发的时候被调用，run 方法首先获取全局的锁，然后检查进程时间服务是否可用，可用的话调用 `target.onProcessingTime(timestamp)`，执行回调函数，最后更新 nextTimestamp，同样，onProcessingTime 方法接收的参数为 initialDelay -> initialDelay + period -> ...
	
	```java
	public ScheduledFuture<?> scheduleAtFixedRate(ProcessingTimeCallback callback, long initialDelay, long period) {
		long nextTimestamp = getCurrentProcessingTime() + initialDelay;

		// 我们尝试注册定时器
		return timerService.scheduleAtFixedRate(
			new RepeatedTriggerTask(status, task, checkpointLock, callback, nextTimestamp, period),
			initialDelay,
			period,
			TimeUnit.MILLISECONDS);
	}
	
	private static final class RepeatedTriggerTask implements Runnable {

		private final AtomicInteger serviceStatus;
		private final Object lock;
		private final ProcessingTimeCallback target;
		private final long period;
		private final AsyncExceptionHandler exceptionHandler;

		private long nextTimestamp;

		private RepeatedTriggerTask(
				final AtomicInteger serviceStatus,
				final AsyncExceptionHandler exceptionHandler,
				final Object lock,
				final ProcessingTimeCallback target,
				final long nextTimestamp,
				final long period) {

			this.serviceStatus = Preconditions.checkNotNull(serviceStatus);
			this.lock = Preconditions.checkNotNull(lock);
			this.target = Preconditions.checkNotNull(target);
			this.period = period;
			this.exceptionHandler = Preconditions.checkNotNull(exceptionHandler);

			this.nextTimestamp = nextTimestamp;
		}

		@Override
		public void run() {
			synchronized (lock) {
				if (serviceStatus.get() == STATUS_ALIVE) {
					target.onProcessingTime(nextTimestamp);
				}

				nextTimestamp += period;
			}
		}
	}
	```
	
## InternalTimerService

InternalTimerService 是一个接口，用于返回当前的进程时间、Watermark 以及注册和删除进程/事件时间定时器，InternalTimerService 是 TimerService 的内部版本，允许通过 key 和 namespace 来明确定时器的所属

```java
public interface InternalTimerService<N> {
	// 返回当前的进程时间
	long currentProcessingTime();
	
	// 返回当前的 event-time watermark
	long currentWatermark();

	/**
	 * 注册一个定时器，当进程时间大于给定的时间的时候，触发定时器
	 * 当定时器触发的时候 namespace 参数会被提供
	 */
	void registerProcessingTimeTimer(N namespace, long time);

	/**
	 * 删除进程定时器
	 */
	void deleteProcessingTimeTimer(N namespace, long time);

	/**
	 * 注册一个定时器，当 event-time watermark 的 ts 大于给定的时间的时候触发
	 * 当定时器触发的时候 namespace 参数会被提供
	 */
	void registerEventTimeTimer(N namespace, long time);

	/**
	 * 删除时间定时器
	 */
	void deleteEventTimeTimer(N namespace, long time);
}
```

## InternalTimerServiceImpl

InternalTimerServiceImpl 是 InternalTimerService 接口的实现类

* InternalTimerServiceImpl 的重要属性

	```java
	// ProcessingTimeService 就是前文讲过的 SystemProcessingTimeService
	private final ProcessingTimeService processingTimeService;
	
	// KeyContext.getCurrentKey() 提供获取当前 key 的接口
	// 只有 KeyedStream 能够操作定时器，因为需要
	// KeyedStateBackend 在检查点的时候存储定时器状态
	private final KeyContext keyContext;
	
	// 存储进程时间定时器的堆，是一个最小堆，触发时间早的位于堆顶
	private final KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<K, N>> processingTimeTimersQueue;
	
	// 存储事件时间定时器的堆，是一个最小堆，触发时间早的位于堆顶
	private final KeyGroupedInternalPriorityQueue<TimerHeapInternalTimer<K, N>> eventTimeTimersQueue;
	
	// 存储流入定时器的 Watermark
	private long currentWatermark = Long.MIN_VALUE;
	
	// 存储进程/事件时间定时器触发的时候的回调
	private Triggerable<K, N> triggerTarget;
	```

* currentProcessingTime

	currentProcessingTime 通过调用 processingTimeService 的 getCurrentProcessingTime 方法返回当前的进程时间
	
	```java
	public long currentProcessingTime() {
		return processingTimeService.getCurrentProcessingTime();
	}
	```

* currentWatermark

	currentWatermark 返回当前定时器的 Watermark，currentWatermark 在 Watermark 流入定时器的时候更新

	```java
	public long currentWatermark() {
		return currentWatermark;
	}
	```

* registerProcessingTimeTimer

	 registerProcessingTimeTimer 注册一个进程时间定时器，当进程时间超过 time 的时候，定时器会被触发，namespace 用于标识定时器的所属（在窗口操作中，每一个窗口都是 namespace），如果不需要 namespace，可以传入 VoidNamespace.INSTANCE
	 
	 当调用 registerProcessingTimeTimer 的时候，会获取 processingTimeTimersQueue 堆的堆顶定时器（oldHead），比较这次要注册的定时器的 timestamp 和 oldHead 的 timestamp，如果 timestamp 早于 oldHead.timestamp，会取消之前在 processingTimeService 上注册的定时器，然后调用 `processingTimeService.registerTimer(timestamp, this)` 重新注册定时器，registerTimer 中回调函数传入了 this，说明定时器触发的时候会调用本类的 onProcessingTime 方法

	```java
	public void registerProcessingTimeTimer(N namespace, long time) {
		InternalTimer<K, N> oldHead = processingTimeTimersQueue.peek();
		if (processingTimeTimersQueue.add(new TimerHeapInternalTimer<>(time, (K) keyContext.getCurrentKey(), namespace))) {
			// 获取当前堆顶的定时器触发时间
			long nextTriggerTime = oldHead != null ? oldHead.getTimestamp() : Long.MAX_VALUE;
			// 检查我们是否需要更早的调度定时器
			if (time < nextTriggerTime) {
				if (nextTimer != null) {
					nextTimer.cancel(false);
				}
				// 重新注册定时器，注册在 processingTimeService 上
				nextTimer = processingTimeService.registerTimer(time, this);
			}
		}
	}
	```

* onProcessingTime

	onProcessingTime 方法用于在进程时间定时器触发的时候执行 triggerTarget 中的回调函数 —— triggerTarget.onProcessingTime，记住这里不要混淆，processingTimeService 在定时器触发的时候调用 onProcessingTime 方法，onProcessingTime 方法调用了  triggerTarget.onProcessingTime 方法

	```java
	public void onProcessingTime(long time) throws Exception {
		// 先将 nextTimer 设置为 null
		// 防止 triggerTarget.onProcessingTime 调用 registerProcessingTimeTimer
		// 最后再在 processingTimeService 上注册新的定时器
		nextTimer = null;

		InternalTimer<K, N> timer;

		// 将所有进程时间定时器的 ts <= time 的都触发
		// 这里的 while 循环保证了快照恢复过来的定时器也能被触发
		while ((timer = processingTimeTimersQueue.peek()) != null && timer.getTimestamp() <= time) {
			processingTimeTimersQueue.poll();
			keyContext.setCurrentKey(timer.getKey());
			triggerTarget.onProcessingTime(timer);
		}

		if (timer != null && nextTimer == null) {
			nextTimer = processingTimeService.registerTimer(timer.getTimestamp(), this);
		}
	}
	```

* registerEventTimeTimer

	registerEventTimeTimer 方法用于注册事件时间定时器，由 Watermark 控制，只有到达定时器的 Watermark 的 timestamp 大于事件时间定时器的 timestamp，事件时间定时器才会被触发
	
	```java
	public void registerEventTimeTimer(N namespace, long time) {
		eventTimeTimersQueue.add(new TimerHeapInternalTimer<>(time, (K) keyContext.getCurrentKey(), namespace));
	}
	```
	
* advanceWatermark

	advanceWatermark 用于更新 currentWatermark，同时触发事件时间定时器，advanceWatermark 会在 Watermark 流入 StreamOperator 的时候被调用

	```java
	public void advanceWatermark(long time) throws Exception {
		currentWatermark = time;

		InternalTimer<K, N> timer;
		// 查看是否 eventTimeTimersQueue 有可以触发的定时器
		// 这里的 while 循环保证了快照恢复过来的定时器也能被触发
		while ((timer = eventTimeTimersQueue.peek()) != null && timer.getTimestamp() <= time) {
			eventTimeTimersQueue.poll();
			keyContext.setCurrentKey(timer.getKey());
			triggerTarget.onEventTime(timer);
		}
	}
	```

## InternalTimeServiceManager

InternalTimeServiceManager 用于给 AbstractStreamOperator 操作符提供访问 InternalTimerServiceImpl 的接口，也可以这么认为，InternalTimeServiceManager 集中管理了 InternalTimerServiceImpl

InternalTimeServiceManager 中用一个 map 存储 String 到 InternalTimerServiceImpl 的映射

```java
private final Map<String, InternalTimerServiceImpl<K, ?>> timerServices;
```

* getInternalTimerService

	getInternalTimerService 是提供给 AbstractStreamOperator 调用的接口，用于获取 InternalTimerServiceImpl，getInternalTimerService 根据传入的 name 去 timerServices 中查询是否有对应的 InternalTimerServiceImpl 实例，如果不存在，则创建一个 InternalTimerServiceImpl 实例并加入 timerServices map 中
	
	```java
	public <N> InternalTimerService<N> getInternalTimerService(
		String name,
		TimerSerializer<K, N> timerSerializer,
		Triggerable<K, N> triggerable) {

		InternalTimerServiceImpl<K, N> timerService = registerOrGetTimerService(name, timerSerializer);

		timerService.startTimerService(
			timerSerializer.getKeySerializer(),
			timerSerializer.getNamespaceSerializer(),
			triggerable);

		return timerService;
	}
	
	<N> InternalTimerServiceImpl<K, N> registerOrGetTimerService(String name, TimerSerializer<K, N> timerSerializer) {
		InternalTimerServiceImpl<K, N> timerService = (InternalTimerServiceImpl<K, N>) timerServices.get(name);
		if (timerService == null) {

			timerService = new InternalTimerServiceImpl<>(
				localKeyGroupRange,
				keyContext,
				processingTimeService,
				createTimerPriorityQueue(PROCESSING_TIMER_PREFIX + name, timerSerializer),
				createTimerPriorityQueue(EVENT_TIMER_PREFIX + name, timerSerializer));

			timerServices.put(name, timerService);
		}
		return timerService;
	}
	```
	
* advanceWatermark

	当 Watermark 流入操作符的时候，AbstractStreamOperator 的 processWatermark 方法被调用，processWatermark 方法会调用 InternalTimeServiceManager 的 advanceWatermark 方法，可以看到，
advanceWatermark 方法调用了 map 中每一个 InternalTimerServiceImpl 实例的 advanceWatermark 方法，更新其 currentWatermark 以及触发相应的事件时间定时器

	```java
	public void advanceWatermark(Watermark watermark) throws Exception {
		for (InternalTimerServiceImpl<?, ?> service : timerServices.values()) {
			service.advanceWatermark(watermark.getTimestamp());
		}
	}
	```
	
## 总结

这篇文章讲解了一下 flink 中定时器的使用，希望对大家有所帮助
	