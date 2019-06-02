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