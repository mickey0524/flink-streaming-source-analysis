# flink 的窗口 —— 窗口组件类

由于 flink 窗口操作涉及的代码较多，因此我决定用几篇文章分别针对不同的点进行讲解，这篇文章我们来讲解一下窗口的一些组件类

## Window

Window 是一个抽象类，代表窗口，一个窗口是将元素分组为有限的桶。窗口有一个最大时间戳，意味着在这个时间点，所有属于该窗口的元素都将到达

```java
public abstract class Window {
	/**
	 * 获取仍属于此窗口的最大时间戳
	 */
	public abstract long maxTimestamp();
}
```

TimeWindow 是 Window 的实现类，TimeWindow 是一个时间窗口，代表一段时间，从 start 到 end，[start, end)，左闭右开，start 端点属于窗口，end 端点不属于窗口，下面我们来看看 TimeWindow 的几个重要的方法

* 构造函数

	```java
	private final long start;
	private final long end;

	/**
	 * 构造函数，start 代表左端点，end 代表右端点
	 */
	public TimeWindow(long start, long end) {
		this.start = start;
		this.end = end;
	}
	```

* maxTimestamp

	maxTimestamp 实现 Window 中定义的抽象方法
	
	```java
	/**
	 * 获取属于该窗口的最大时间戳，因为区间是左闭右开，所以是 end - 1
	 */
	public long maxTimestamp() {
		return end - 1;
	}
	```

* getWindowStartWithOffset

	getWindowStartWithOffset 用于获取元素所属窗口的开端，timestamp 指代元素的时间戳，windowSize 指代窗口的大小，offset 指代偏移量
	
	假设 timestamp 为 90，windowSize 为 60，如果 offset 为 0，窗口则为 0 - 60，60 - 120 ... 如果 offset 为 20，窗口贼为 20 - 80，80 - 140 ...

	```java
	/**
	 * 获取窗口的开端
	 */
	public static long getWindowStartWithOffset(long timestamp, long offset, long windowSize) {
		return timestamp - (timestamp - offset + windowSize) % windowSize;
	}
	```

* 窗口合并相关

	相交的窗口可以合并为一个窗口，intersects 方法判断本窗口和 other 窗口是否相交，cover 返回能够覆盖本窗口和给定的窗口的最小窗口

	```java
	/**
	 * 返回窗口是否与 other 窗口相交
	 */
	public boolean intersects(TimeWindow other) {
		return this.start <= other.end && this.end >= other.start;
	}

	/**
	 * 返回最小的窗口能够覆盖本窗口和给定的窗口
	 */
	public TimeWindow cover(TimeWindow other) {
		return new TimeWindow(Math.min(start, other.start), Math.max(end, other.end));
	}
	```
	
	mergeWindows 方法接受一个时间窗口的集合，合并集合中所有相交的时间窗口，得到一个元素为 `Tuple2<TimeWindow, Set<TimeWindow>>` 的 List，Tuple2.f0 指代合并得到的时间窗口，Tuple2.f1 指代合并成 Tuple2.f0 的所有时间窗口组成的 set
	
	```java
	/**
	 * 合并重叠的时间窗口
	 * 会在合并窗口分配器的时候被用到
	 */
	public static void mergeWindows(Collection<TimeWindow> windows, MergingWindowAssigner.MergeCallback<TimeWindow> c) {

		// 将窗口按照开始时间排序，然后合并有重合的窗口
		List<TimeWindow> sortedWindows = new ArrayList<>(windows);

		Collections.sort(sortedWindows, new Comparator<TimeWindow>() {
			@Override
			public int compare(TimeWindow o1, TimeWindow o2) {
				return Long.compare(o1.getStart(), o2.getStart());
			}
		});

		List<Tuple2<TimeWindow, Set<TimeWindow>>> merged = new ArrayList<>();
		Tuple2<TimeWindow, Set<TimeWindow>> currentMerge = null;

		for (TimeWindow candidate: sortedWindows) {
			// currentMerge 为空的时候，初始化，直接赋值
			if (currentMerge == null) {
				currentMerge = new Tuple2<>();
				currentMerge.f0 = candidate;
				currentMerge.f1 = new HashSet<>();
				currentMerge.f1.add(candidate);
			} else if (currentMerge.f0.intersects(candidate)) {
				// 当前窗口和 currentMerge.f0 的窗口相交
				currentMerge.f0 = currentMerge.f0.cover(candidate);
				currentMerge.f1.add(candidate);
			} else {
				// 当不相交的时候，需要新建立 currentMerge
				merged.add(currentMerge);
				currentMerge = new Tuple2<>();
				currentMerge.f0 = candidate;
				currentMerge.f1 = new HashSet<>();
				currentMerge.f1.add(candidate);
			}
		}

		if (currentMerge != null) {
			merged.add(currentMerge);
		}

		for (Tuple2<TimeWindow, Set<TimeWindow>> m: merged) {
			// set 的 size() 大于 1，表明这个 Tuple2 是多个原始窗口合并的
			// 调用 merge 的回调函数
			if (m.f1.size() > 1) {
				c.merge(m.f1, m.f0);
			}
		}
	}
	```

## Time

Time 是窗口中使用的时间单位，由 TimeUnit 和 size 组成，TimeUnit 指代 Time 使用的时间单位，size 指代窗口由多少 TimeUnit 组成

```java
public final class Time {
	// 时间单位
	private final TimeUnit unit;

	// 时间量级
	private final long size;

	private Time(long size, TimeUnit unit) {
		this.unit = checkNotNull(unit, "time unit may not be null");
		this.size = size;
	}
	
	/**
	 * 将窗口长度转为毫秒表示
	 */
	public long toMilliseconds() {
		return unit.toMillis(size);
	}
}
```

## WindowAssigner

WindowAssigner 是窗口分配器，用于给一个元素分配一个或多个窗口

* WindowAssigner 抽象类

	```java
	public abstract class WindowAssigner<T, W extends Window> implements Serializable {
		private static final long serialVersionUID = 1L;

		/**
		 * 返回应该分配给元素的窗口集合
		 *
		 * @param element 被分配窗口的元素
		 * @param timestamp 元素的 ts
		 * @param context 分配器在其中操作的 WindowAssignerContext
		 */
		public abstract Collection<W> assignWindows(T element, long timestamp, WindowAssignerContext context);
	
		/**
		 * 返回与该 WindowAssigner 有关的默认触发器
		 */
		public abstract Trigger<T, W> getDefaultTrigger(StreamExecutionEnvironment env);
	
		/**
		 * 返回一个类型序列器，用来序列化窗口
		 */
		public abstract TypeSerializer<W> getWindowSerializer(ExecutionConfig executionConfig);
	
		/**
		 * 返回元素是否是按照 event time 来分配窗口的
		 */
		public abstract boolean isEventTime();
	
		/**
		 * WindowAssigner 的 context，可以用于请求当前的进程时间
		 */
		public abstract static class WindowAssignerContext {
	
			/**
			 * Returns the current processing time.
			 */
			/**
			 * 返回当前的进程时间
			 */
			public abstract long getCurrentProcessingTime();
	
		}
	}
	```

* TumblingProcessingTimeWindows

	TumblingProcessingTimeWindows 是翻转进程时间窗口分配器，根据机器的当前系统时间，将元素放入窗口中，窗口不能重叠，例如窗口 size 为 60，offset 为 0，0 - 60 为第一个窗口，60 - 120 为第二个，依此类推。
可以看到 assignWindows 方法调用了前文讲过的 `TimeWindow.getWindowStartWithOffset(now, offset, size)` 来获取窗口的开端
	
	```java
	public class TumblingProcessingTimeWindows extends WindowAssigner<Object, TimeWindow> {
		private final long size;  // 窗口大小
	
		private final long offset;  // 窗口偏移
	
		private TumblingProcessingTimeWindows(long size, long offset) {
			// 当 offset 大于 size 的时候，抛出异常
			if (Math.abs(offset) >= size) {
				throw new IllegalArgumentException("TumblingProcessingTimeWindows parameters must satisfy abs(offset) < size");
			}
	
			this.size = size;
			this.offset = offset;
		}
	
		@Override
		// 根据当前进程的时间分配窗口，与 element 本身无关，只取决的于当前的进程时间
		public Collection<TimeWindow> assignWindows(Object element, long timestamp, WindowAssignerContext context) {
			final long now = context.getCurrentProcessingTime();
			// 获取元素应该位于窗口的开端
			long start = TimeWindow.getWindowStartWithOffset(now, offset, size);
			return Collections.singletonList(new TimeWindow(start, start + size));
		}
	}
	```

* TumblingEventTimeWindows

	TumblingEventTimeWindows 是翻转事件时间窗口分配器，根据元素的时间戳，将窗口元素放入窗口中，窗口不能重叠，assignWindows 中校验了 timestamp 是否大于 Long.MIN_VALUE，当采用 ProcessingTime 模式的时候，StreamRecord 的 ts 始终是 Long.MIN\_VALUE，当采用 EventTime 模式的时候，如果 StreamSource 产生元素的时候没有调用 collectWithTimestamp，StreamRecord 最初的 ts 也是 Long.MIN\_VALUE，需要调用 DataStream 的 assignTimestampsAndWatermark 方法来给 StreamRecord 设置 timestamp 以及生成 Watermark
	
	```java
	public class TumblingEventTimeWindows extends WindowAssigner<Object, TimeWindow> {
		private static final long serialVersionUID = 1L;
	
		private final long size;  // 窗口大小
	
		private final long offset;  // 窗口偏移
	
		protected TumblingEventTimeWindows(long size, long offset) {
			if (Math.abs(offset) >= size) {
				throw new IllegalArgumentException("TumblingEventTimeWindows parameters must satisfy abs(offset) < size");
			}
	
			this.size = size;
			this.offset = offset;
		}
	
		@Override
		public Collection<TimeWindow> assignWindows(Object element, long timestamp, WindowAssignerContext context) {
			if (timestamp > Long.MIN_VALUE) {
				// 当 StreamRecord 没有 ts 的时候，getTimestamp 返回 Long.MIN_VALUE
				long start = TimeWindow.getWindowStartWithOffset(timestamp, offset, size);
				return Collections.singletonList(new TimeWindow(start, start + size));
			} else {
				// 说明 StreamRecord 没有被设置 ts，有空吗没有调用 assignTimestampsAndWatermarks
				throw new RuntimeException("Record has Long.MIN_VALUE timestamp (= no timestamp marker). " +
						"Is the time characteristic set to 'ProcessingTime', or did you forget to call " +
						"'DataStream.assignTimestampsAndWatermarks(...)'?");
			}
		}
	}
	```
	
* SlidingProcessingTimeWindows

	SlidingProcessingTimeWindows 是滑动进程时间窗口分配器，根据机器的当前系统时间，将元素放入窗口中，窗口可以重叠，例如窗口 size 为 60，offset 为 0，slide 为 10，0 - 60 为第一个窗口，10 - 70 为第二个，依此类推。SlidingProcessingTimeWindows 中 offset 的绝对值不能大于 slide，因为在调用 `TimeWindow.getWindowStartWithOffset` 的时候，传入的 size 是 slide

	```java
	public class SlidingProcessingTimeWindows extends WindowAssigner<Object, TimeWindow> {	
		private final long size;
	
		private final long offset;
	
		private final long slide;
	
		private SlidingProcessingTimeWindows(long size, long slide, long offset) {
			if (Math.abs(offset) >= slide || size <= 0) {
				throw new IllegalArgumentException("SlidingProcessingTimeWindows parameters must satisfy " +
					"abs(offset) < slide and size > 0");
			}
	
			this.size = size;
			this.slide = slide;
			this.offset = offset;
		}
	
		@Override
		/**
		 * 分配元素属于的窗口
		 */
		public Collection<TimeWindow> assignWindows(Object element, long timestamp, WindowAssignerContext context) {
			timestamp = context.getCurrentProcessingTime();
			// 每一个元素都应该至少属于 size/slide 个滑动窗口，需要记住，时间窗口都是左闭右开的
			List<TimeWindow> windows = new ArrayList<>((int) (size / slide));
			long lastStart = TimeWindow.getWindowStartWithOffset(timestamp, offset, slide);
			// 这里感觉能优化一下，start 有可能左溢出，比如 timestamp 为 3，size 为 60
			for (long start = lastStart;
				start > timestamp - size;
				start -= slide) {
				windows.add(new TimeWindow(start, start + size));
			}
			return windows;
		}
	}
	```

* SlidingEventTimeWindows

	SlidingEventTimeWindows 和 SlidingProcessingTimeWindows 分配窗口的方式相同，SlidingEventTimeWindows 依赖于元素的 timestamp，SlidingProcessingTimeWindows 依赖于当前的进程时间

	```java
	public class SlidingEventTimeWindows extends WindowAssigner<Object, TimeWindow> {

	private final long size;

	private final long slide;

	private final long offset;

	protected SlidingEventTimeWindows(long size, long slide, long offset) {
		if (Math.abs(offset) >= slide || size <= 0) {
			throw new IllegalArgumentException("SlidingEventTimeWindows parameters must satisfy " +
				"abs(offset) < slide and size > 0");
		}

		this.size = size;
		this.slide = slide;
		this.offset = offset;
	}

	@Override
	/**
	 * 依据元素的时间戳给元素分配窗口
	 */
	public Collection<TimeWindow> assignWindows(Object element, long timestamp, WindowAssignerContext context) {
		if (timestamp > Long.MIN_VALUE) {
			List<TimeWindow> windows = new ArrayList<>((int) (size / slide));
			long lastStart = TimeWindow.getWindowStartWithOffset(timestamp, offset, slide);
			for (long start = lastStart;
				start > timestamp - size;
				start -= slide) {
				windows.add(new TimeWindow(start, start + size));
			}
			return windows;
		} else {
			throw new RuntimeException("Record has Long.MIN_VALUE timestamp (= no timestamp marker). " +
					"Is the time characteristic set to 'ProcessingTime', or did you forget to call " +
					"'DataStream.assignTimestampsAndWatermarks(...)'?");
		}
	}
	```

* MergingWindowAssigner

	MergingWindowAssigner 是一个抽象类，定义了一种能够合并窗口的窗口分配器，MergingWindowAssigner 的实现类通过调用 TimeWindow 的 mergeWindows 实现 mergeWindows 方法
	
	```java
	public abstract class MergingWindowAssigner<T, W extends Window> extends WindowAssigner<T, W> {

		/**
		 * 决定了哪些窗口应该被合并
		 */
		public abstract void mergeWindows(Collection<W> windows, MergeCallback<W> callback);
	
		/**
		 * mergeWindows 中使用的回调函数
		 */
		public interface MergeCallback<W> {
	
			/**
			 * toBeMerged 是被合并的窗口的集合
			 * mergeResult 是 toBeMerged 中窗口合并的结果
			 */
			void merge(Collection<W> toBeMerged, W mergeResult);
		}
	}
	```

* ProcessingTimeSessionWindows

	ProcessingTimeSessionWindows 是一种 MergingWindowAssigner，根据当前的进程时间和设置的 sessionTimeout 为元素分配窗口，ProcessingTimeSessionWindows 不会去找窗口的开端，而是将当前的进程时间作为开端，sessionTimeout 作为大小。ProcessingTimeSessionWindows 通过调用 TimeWindow 的 mergeWindows 实现 mergeWindows 方法（其他 MergingWindowAssigner 的代码中就不给出这个方法了）

	```java
	public class ProcessingTimeSessionWindows extends MergingWindowAssigner<Object, TimeWindow> {
		protected long sessionTimeout;
	
		protected ProcessingTimeSessionWindows(long sessionTimeout) {
			if (sessionTimeout <= 0) {
				throw new IllegalArgumentException("ProcessingTimeSessionWindows parameters must satisfy 0 < size");
			}
	
			this.sessionTimeout = sessionTimeout;
		}
	
		@Override
		/**
		 * 为元素分配窗口，窗口与元素本身无关，只与当前的进程时间和设置的 sessionTimeout 有关
		 */
		public Collection<TimeWindow> assignWindows(Object element, long timestamp, WindowAssignerContext context) {
			long currentProcessingTime = context.getCurrentProcessingTime();
			return Collections.singletonList(new TimeWindow(currentProcessingTime, currentProcessingTime + sessionTimeout));
		}

		public void mergeWindows(Collection<TimeWindow> windows, MergeCallback<TimeWindow> c) {
			TimeWindow.mergeWindows(windows, c);
		}
	}
	```

* EventTimeSessionWindows

	和 ProcessingTimeSessionWindows 类似，使用元素的时间戳作为窗口的开端

	```java
	public class EventTimeSessionWindows extends MergingWindowAssigner<Object, TimeWindow> {
		protected long sessionTimeout;
	
		/**
		 * 构造函数
		 */
		protected EventTimeSessionWindows(long sessionTimeout) {
			if (sessionTimeout <= 0) {
				throw new IllegalArgumentException("EventTimeSessionWindows parameters must satisfy 0 < size");
			}
	
			this.sessionTimeout = sessionTimeout;
		}
	
		@Override
		public Collection<TimeWindow> assignWindows(Object element, long timestamp, WindowAssignerContext context) {
			return Collections.singletonList(new TimeWindow(timestamp, timestamp + sessionTimeout));
		}
	}
	```
	
* DynamicProcessingTimeSessionWindows
	
	DynamicProcessingTimeSessionWindows 和 ProcessingTimeSessionWindows 类似，都是通过当前的进程时间和一个 sessionTimeout 来分配端口，不过 DynamicProcessingTimeSessionWindows 调用 `sessionWindowTimeGapExtractor.extract(element)` 获取 sessionTimeout，针对不同的元素，有可能得到不同的 sessionTimeout
	
	```java
	public class DynamicProcessingTimeSessionWindows<T> extends MergingWindowAssigner<T, TimeWindow> {
		protected SessionWindowTimeGapExtractor<T> sessionWindowTimeGapExtractor;
	
		protected DynamicProcessingTimeSessionWindows(SessionWindowTimeGapExtractor<T> sessionWindowTimeGapExtractor) {
			this.sessionWindowTimeGapExtractor = sessionWindowTimeGapExtractor;
		}
	
		@Override
		public Collection<TimeWindow> assignWindows(T element, long timestamp, WindowAssignerContext context) {
			long currentProcessingTime = context.getCurrentProcessingTime();
			// 这个 sessionTimeout 和 ProcessingTimeSessionWindows.java 不一样
			// 这个是根据 element 动态获取 sessionTimeout
			long sessionTimeout = sessionWindowTimeGapExtractor.extract(element);
			if (sessionTimeout <= 0) {
				throw new IllegalArgumentException("Dynamic session time gap must satisfy 0 < gap");
			}
			return Collections.singletonList(new TimeWindow(currentProcessingTime, currentProcessingTime + sessionTimeout));
		}
	}
	```
	
* DynamicEventTimeSessionWindows

	DynamicEventTimeSessionWindows 和 DynamicProcessingTimeSessionWindows 类似，使用元素的时间戳作为窗口的开端
	
	```java
	public class DynamicEventTimeSessionWindows<T> extends MergingWindowAssigner<T, TimeWindow> {
		protected SessionWindowTimeGapExtractor<T> sessionWindowTimeGapExtractor;
	
		protected DynamicEventTimeSessionWindows(SessionWindowTimeGapExtractor<T> sessionWindowTimeGapExtractor) {
			this.sessionWindowTimeGapExtractor = sessionWindowTimeGapExtractor;
		}
	
		@Override
		public Collection<TimeWindow> assignWindows(T element, long timestamp, WindowAssignerContext context) {
			// sessionTimeout 是根据元素动态调整了
			long sessionTimeout = sessionWindowTimeGapExtractor.extract(element);
			if (sessionTimeout <= 0) {
				throw new IllegalArgumentException("Dynamic session time gap must satisfy 0 < gap");
			}
			return Collections.singletonList(new TimeWindow(timestamp, timestamp + sessionTimeout));
		}
	}
	```

## Trigger

Trigger 是一个触发器，用来决定何时计算窗口 emit 该窗口部分的结果

* Trigger

	Trigger 是一个抽象类，定义了如下所示的多个方法，每个方法在代码块中都有详细的注释

	```java
	public abstract class Trigger<T, W extends Window> implements Serializable {

		/**
		 * 为窗格中的每个元素调用 onElement 方法，方法的返回值决定了
		 * 是否窗格被执行来输出结果
		 */
		public abstract TriggerResult onElement(T element, long timestamp, W window, TriggerContext ctx) throws Exception;
	
		/**
		 * 使用 ctx 创建一个进程时间定时器，定时器触发的时候调用
		 */
		public abstract TriggerResult onProcessingTime(long time, W window, TriggerContext ctx) throws Exception;

		/**
		 * 使用 ctx 来创建事件时间定时器，定时器触发的时候调用
		 */
		public abstract TriggerResult onEventTime(long time, W window, TriggerContext ctx) throws Exception;

		/**
		 * 当 trigger 支持 trigger 状态合并的时候返回 true
		 * 返回 true 代表可以与 MergingWindowAssigner 一起使用
		 * 同时也需要实现 onMerge 方法
		 */
		public boolean canMerge() {
			return false;
		}

		/**
		 * 多个窗口被合并成一个窗口的时候调用
		 */
		public void onMerge(W window, OnMergeContext ctx) throws Exception {
			throw new UnsupportedOperationException("This trigger does not support merging.");
		}
	
		/**
		 * 清除触发器为窗口保留的所有状态
		 * 当窗口被清除的时候调用这个方法
		 * 用 registerEventTimeTimer 或 registerProcessingTimeTimer 设置的定时器需要在这里被删除
		 * 用 getPartitionedState 获取的状态也需要被删除
		 */
		public abstract void clear(W window, TriggerContext ctx) throws Exception;
	
		// ------------------------------------------------------------------------
	
		/**
		 * A context object that is given to {@link Trigger} methods to allow them to register timer
		 * callbacks and deal with state.
		 */
		/**
		 * 一个 trigger 上下文
		 * trigger 可以使用 ctx 来注册定时器以及处理 state
		 */
		public interface TriggerContext {
	
			/**
			 * 返回当前的进程时间
			 */
			long getCurrentProcessingTime();
	
			/**
			 * 返回当前的 watermark 时间
			 */
			long getCurrentWatermark();

			/**
			 * 注册一个系统时间回调。如果当前的系统时间大于注册的时候
			 * onProcessingTime 被调用
			 */
			void registerProcessingTimeTimer(long time);

			/**
			 * 注册一个事件时间回调。如果当前的事件时间大于注册的时候
			 * onEventTime 被调用
			 */
			void registerEventTimeTimer(long time);
	
			/**
			 * 删除给定时间的进程时间触发器
			 */
			void deleteProcessingTimeTimer(long time);
	
			/**
			 * 删除给定时间的事件事件触发器
			 */
			void deleteEventTimeTimer(long time);

			/**
			 * 检索可用于与容错状态交互的状态对象，容错状态的作用域是当前触发器调用的窗口（窗口用作 namespace）和键
			 */
			<S extends State> S getPartitionedState(StateDescriptor<S, ?> stateDescriptor);
		}

		/**
		 * TriggerContext 的扩展
		 * 在 onMerge 的时候使用
		 */
		public interface OnMergeContext extends TriggerContext {
			<S extends MergingState<?, ?>> void mergePartitionedState(StateDescriptor<S, ?> stateDescriptor);
		}
	}
	```

* ProcessingTimeTrigger

	ProcessingTimeTrigger 是一种触发器，当机器的进程时间大于窗口的末端时触发，TriggerResult.CONTINUE 代表不触发，TriggerResult.Fire 代表触发
	
	```java
	public class ProcessingTimeTrigger extends Trigger<Object, TimeWindow> {
		private ProcessingTimeTrigger() {}
	
		// 以窗口的末端时间戳设置进程时间定时器
		@Override
		public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) {
			ctx.registerProcessingTimeTimer(window.maxTimestamp());
			return TriggerResult.CONTINUE;
		}
	
		@Override
		public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
			return TriggerResult.CONTINUE;
		}
		
		// 当定时器触发的时候，表明窗口可以被触发了
		@Override
		public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) {
			return TriggerResult.FIRE;
		}
	
		@Override
		public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
			ctx.deleteProcessingTimeTimer(window.maxTimestamp());
		}
	
		@Override
		public boolean canMerge() {
			return true;
		}
	
		@Override
		public void onMerge(TimeWindow window,
				OnMergeContext ctx) {
			// 只有当时间还没有超过合并窗口的末端时才注册定时器
			long windowMaxTimestamp = window.maxTimestamp();
			if (windowMaxTimestamp > ctx.getCurrentProcessingTime()) {
				ctx.registerProcessingTimeTimer(windowMaxTimestamp);
			}
		}
	}
	```

* EventTimeTrigger

	EventTimeTrigger 是一种触发器，当 Watermark 的 timestamp 大于窗口的末端时触发

	```java
	public class EventTimeTrigger extends Trigger<Object, TimeWindow> {
		private EventTimeTrigger() {}
	
		@Override
		public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
			if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
				// 如果 watermark 已经超过窗口末端了，立即触发定时器
				return TriggerResult.FIRE;
			} else {
				// 否则注册事件时间定时器，在 onEventTime 中触发ctx.registerEventTimeTimer(window.maxTimestamp());
				return TriggerResult.CONTINUE;
			}
		}
		
		@Override
		public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) {
			return time == window.maxTimestamp() ?
				TriggerResult.FIRE :
				TriggerResult.CONTINUE;
		}
	
		@Override
		public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
			return TriggerResult.CONTINUE;
		}
	
		@Override
		public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
			ctx.deleteEventTimeTimer(window.maxTimestamp());
		}
	
		@Override
		public boolean canMerge() {
			return true;
		}
	
		@Override
		public void onMerge(TimeWindow window,
				OnMergeContext ctx) {
			// 只有当 watermark 还没有超过合并窗口的末端时才注册定时器
			long windowMaxTimestamp = window.maxTimestamp();
			if (windowMaxTimestamp > ctx.getCurrentWatermark()) {
				ctx.registerEventTimeTimer(windowMaxTimestamp);
			}
		}
	}
	```

* DeltaTrigger

	DeltaTrigger 是一种基于 DeltaFunction 和阈值的触发器，DeltaTrigger 计算上次触发的数据点和当前到达的数据点之间的增量，如果增量高于指定阈值，则触发。DeltaTrigger 不需要注册定时器，因此 onEventTime 方法和 onProcessingTime 方法直接返回 TriggerResult.CONTINUE
	
	由于需要和上次触发的数据点比对，出于检查点的考虑，DeltaTrigger 在构造函数中实例化了一个 ValueStateDescriptor，可以把 ValueStateDescriptor 理解为一个 key，用 `ctx.getPartitionedState(ValueStateDescriptor)` 可以获取存储的 State，DeltaTrigger 用 State 来存储上一次触发的数据点
	
	DeltaTrigger 不能被 merge，所以没有实现 onMerge 方法
	
	先来看看 DeltaFunction
	
	```java
	public interface DeltaFunction<DATA> extends Serializable {
		// 计算给定的两个数据点之间的增量
		double getDelta(DATA oldDataPoint, DATA newDataPoint);
	}
	```
	
	再看看 DeltaTrigger 的源码
	
	```java
	public class DeltaTrigger<T, W extends Window> extends Trigger<T, W> {
		private final DeltaFunction<T> deltaFunction;
		private final double threshold;
		private final ValueStateDescriptor<T> stateDesc;
	
		private DeltaTrigger(double threshold, DeltaFunction<T> deltaFunction, TypeSerializer<T> stateSerializer) {
			this.deltaFunction = deltaFunction;
			this.threshold = threshold;
			stateDesc = new ValueStateDescriptor<>("last-element", stateSerializer);
		}
	
		@Override
		public TriggerResult onElement(T element, long timestamp, W window, TriggerContext ctx) throws Exception {
			ValueState<T> lastElementState = ctx.getPartitionedState(stateDesc);  // 获取上次的数据状态
			// 如果没有上次的元素状态，更新元素状态
			if (lastElementState.value() == null) {
				lastElementState.update(element);
				return TriggerResult.CONTINUE;
			}
			// 如果上次触发的数据点和当前到达的数据点之间的增量大于域值，触发
			if (deltaFunction.getDelta(lastElementState.value(), element) > this.threshold) {
				lastElementState.update(element);
				return TriggerResult.FIRE;
			}
			return TriggerResult.CONTINUE;
		}
	
		@Override
		public TriggerResult onEventTime(long time, W window, TriggerContext ctx) {
			return TriggerResult.CONTINUE;
		}
	
		@Override
		public TriggerResult onProcessingTime(long time, W window, TriggerContext ctx) throws Exception {
			return TriggerResult.CONTINUE;
		}
	
		@Override
		public void clear(W window, TriggerContext ctx) throws Exception {
			ctx.getPartitionedState(stateDesc).clear();
		}
	}
	```

* CountTrigger

	CountTrigger 是一种计数触发器，当窗口中元素数量到达给定的数值的时候，窗口被触发，和 DeltaTrigger 类似，CountTrigger 同样不依赖定时器，CountTrigger 也需要一个 ReducingStateDescriptor 来保存当前窗口中的元素数量，ReducingStateDescriptor 接收 Sum 实例，在一个元素到来的时候，`count.add(1L)` 会给 State 中的值加上一

	```java
	public class CountTrigger<W extends Window> extends Trigger<Object, W> {
		private final long maxCount;
	
		private final ReducingStateDescriptor<Long> stateDesc =
				new ReducingStateDescriptor<>("count", new Sum(), LongSerializer.INSTANCE);
	
		private CountTrigger(long maxCount) {
			this.maxCount = maxCount;
		}
	
		@Override
		public TriggerResult onElement(Object element, long timestamp, W window, TriggerContext ctx) throws Exception {
			// 获取当前的计数状态
			ReducingState<Long> count = ctx.getPartitionedState(stateDesc);
			count.add(1L);
			// 当前的计数大于 maxCount 时候，触发
			if (count.get() >= maxCount) {
				count.clear();
				return TriggerResult.FIRE;
			}
			return TriggerResult.CONTINUE;
		}
	
		@Override
		public TriggerResult onEventTime(long time, W window, TriggerContext ctx) {
			return TriggerResult.CONTINUE;
		}
	
		@Override
		public TriggerResult onProcessingTime(long time, W window, TriggerContext ctx) throws Exception {
			return TriggerResult.CONTINUE;
		}
	
		@Override
		public void clear(W window, TriggerContext ctx) throws Exception {
			ctx.getPartitionedState(stateDesc).clear();
		}
	
		@Override
		public boolean canMerge() {
			return true;
		}
	
		@Override
		public void onMerge(W window, OnMergeContext ctx) throws Exception {
			ctx.mergePartitionedState(stateDesc);
		}
	}
	
	private static class Sum implements ReduceFunction<Long> {
		private static final long serialVersionUID = 1L;

		@Override
		public Long reduce(Long value1, Long value2) throws Exception {
			return value1 + value2;
		}

	}
	```

* ContinuousProcessingTimeTrigger

	ContinuousProcessingTimeTrigger 是一种根据给定的时间间隔连续触发的触发器，ContinuousProcessingTimeTrigger 使用 ReducingStateDescriptor 接收一个 Min 实例来保存触发时间中的最小值，onProcessingTime 中会再次注册进程时间定时器

	```java
	public class ContinuousProcessingTimeTrigger<W extends Window> extends Trigger<Object, W> {
	
		private final long interval;
	
		// 当合并的时候，我们选择所有触发时间中的最小值作为新的触发时间
		private final ReducingStateDescriptor<Long> stateDesc =
				new ReducingStateDescriptor<>("fire-time", new Min(), LongSerializer.INSTANCE);
	
		private ContinuousProcessingTimeTrigger(long interval) {
			this.interval = interval;
		}
	
		@Override
		public TriggerResult onElement(Object element, long timestamp, W window, TriggerContext ctx) throws Exception {
			ReducingState<Long> fireTimestamp = ctx.getPartitionedState(stateDesc);
	
			timestamp = ctx.getCurrentProcessingTime();
	
			// 第一次注册，之后都会在 onProcessingTime 的时候再次注册
			if (fireTimestamp.get() == null) {
				long start = timestamp - (timestamp % interval);
				long nextFireTimestamp = start + interval;  // 计算下一次触发的时间
	
				ctx.registerProcessingTimeTimer(nextFireTimestamp);
	
				fireTimestamp.add(nextFireTimestamp);
				return TriggerResult.CONTINUE;
			}
			return TriggerResult.CONTINUE;
		}
	
		@Override
		public TriggerResult onEventTime(long time, W window, TriggerContext ctx) throws Exception {
			return TriggerResult.CONTINUE;
		}
	
		@Override
		public TriggerResult onProcessingTime(long time, W window, TriggerContext ctx) throws Exception {
			ReducingState<Long> fireTimestamp = ctx.getPartitionedState(stateDesc);
	
			if (fireTimestamp.get().equals(time)) {
				fireTimestamp.clear();
				fireTimestamp.add(time + interval);
				ctx.registerProcessingTimeTimer(time + interval);  // 再次注册
				return TriggerResult.FIRE;
			}
			return TriggerResult.CONTINUE;
		}
	
		@Override
		public void clear(W window, TriggerContext ctx) throws Exception {
			ReducingState<Long> fireTimestamp = ctx.getPartitionedState(stateDesc);
			long timestamp = fireTimestamp.get();
			ctx.deleteProcessingTimeTimer(timestamp);  // 删除最后注册的定时器
			fireTimestamp.clear();  // 清空状态
		}
	
		@Override
		public boolean canMerge() {
			return true;
		}
	
		@Override
		public void onMerge(W window,
				OnMergeContext ctx) {
			ctx.mergePartitionedState(stateDesc);
		}
	}
	
	private static class Min implements ReduceFunction<Long> {
		private static final long serialVersionUID = 1L;

		@Override
		public Long reduce(Long value1, Long value2) throws Exception {
			return Math.min(value1, value2);
		}
	}
	```

* ContinuousEventTimeTrigger

	ContinuousEventTimeTrigger 和 ContinuousProcessingTimeTrigger 基本一样，这里就不介绍了，大家可以结合着代码自行看看

	```java
	public class ContinuousEventTimeTrigger<W extends Window> extends Trigger<Object, W> {
		private static final long serialVersionUID = 1L;
	
		private final long interval;
	
		// 当合并的时候，我们选择所有触发时间中的最小值作为新的触发时间
		private final ReducingStateDescriptor<Long> stateDesc =
				new ReducingStateDescriptor<>("fire-time", new Min(), LongSerializer.INSTANCE);
	
		private ContinuousEventTimeTrigger(long interval) {
			this.interval = interval;
		}
	
		@Override
		public TriggerResult onElement(Object element, long timestamp, W window, TriggerContext ctx) throws Exception {
	
			if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
				// if the watermark is already past the window fire immediately
				// 如果 watermark 已经大于窗口末端，立即触发
				return TriggerResult.FIRE;
			} else {
				ctx.registerEventTimeTimer(window.maxTimestamp());
			}
	
			ReducingState<Long> fireTimestamp = ctx.getPartitionedState(stateDesc);
			if (fireTimestamp.get() == null) {
				long start = timestamp - (timestamp % interval);
				long nextFireTimestamp = start + interval;
				ctx.registerEventTimeTimer(nextFireTimestamp);
				fireTimestamp.add(nextFireTimestamp);
			}
	
			return TriggerResult.CONTINUE;
		}
	
		@Override
		public TriggerResult onEventTime(long time, W window, TriggerContext ctx) throws Exception {
	
			if (time == window.maxTimestamp()){
				return TriggerResult.FIRE;
			}
	
			ReducingState<Long> fireTimestampState = ctx.getPartitionedState(stateDesc);
	
			Long fireTimestamp = fireTimestampState.get();
	
			if (fireTimestamp != null && fireTimestamp == time) {
				fireTimestampState.clear();
				fireTimestampState.add(time + interval);
				ctx.registerEventTimeTimer(time + interval);
				return TriggerResult.FIRE;
			}
	
			return TriggerResult.CONTINUE;
		}
	
		@Override
		public TriggerResult onProcessingTime(long time, W window, TriggerContext ctx) throws Exception {
			return TriggerResult.CONTINUE;
		}
	
		@Override
		public void clear(W window, TriggerContext ctx) throws Exception {
			ReducingState<Long> fireTimestamp = ctx.getPartitionedState(stateDesc);
			Long timestamp = fireTimestamp.get();
			if (timestamp != null) {
				ctx.deleteEventTimeTimer(timestamp);
				fireTimestamp.clear();
			}
		}
	
		@Override
		public boolean canMerge() {
			return true;
		}
	
		@Override
		public void onMerge(W window, OnMergeContext ctx) throws Exception {
			ctx.mergePartitionedState(stateDesc);
			Long nextFireTimestamp = ctx.getPartitionedState(stateDesc).get();
			if (nextFireTimestamp != null) {
				ctx.registerEventTimeTimer(nextFireTimestamp);
			}
		}
	}
	
	private static class Min implements ReduceFunction<Long> {
		private static final long serialVersionUID = 1L;

		@Override
		public Long reduce(Long value1, Long value2) throws Exception {
			return Math.min(value1, value2);
		}
	}
	```
	
## Evictor
 
Evictor 用于在窗口函数执行前后删除部分窗格内的元素

* Evictor

	Evictor 是一个抽象类，定义了如下所示的多个方法，每个方法在代码块中都有详细的注释

	```java
	public interface Evictor<T, W extends Window> extends Serializable {
	
		/**
		 * 驱逐部分元素，在窗口函数执行前调用
		 */
		void evictBefore(Iterable<TimestampedValue<T>> elements, int size, W window, EvictorContext evictorContext);
	
		/**
		 * 驱逐部分元素，在窗口函数执行后调用
		 */
		void evictAfter(Iterable<TimestampedValue<T>> elements, int size, W window, EvictorContext evictorContext);
	
		/**
		 * Evictor 方法的上下文对象
		 */
		interface EvictorContext {
	
			/**
			 * 返回当前的进程时间
			 */
			long getCurrentProcessingTime();

			/**
			 * 返回当前的 watermark
			 */
			long getCurrentWatermark();
		}
	}
	```
	
* TimeEvictor

	TimeEvictor 针对元素的时间戳判断是否驱逐，TimeEvictor 有一个 windowSize 的属性，用于定时窗口最大能保存多少时间范围的元素，在 evict 方法中，TimeEvictor 从所有的元素中获取最大时间戳（currentTime），currentTime - windowSize 就是保留的元素的最低时间限制，然后 TimeEvictor 遍历所有元素，remove 所有时间戳小于 currentTime - windowSize 的元素

	```java
	private final long windowSize;  // 窗口大小
	private final boolean doEvictAfter;  // 在窗口函数执行前还是后执行驱逐操作
	
	// Evictor 抽象类的 evictBefore 和 evictAfter 方法都会调用 evict 方法
	private void evict(Iterable<TimestampedValue<Object>> elements, int size, EvictorContext ctx) {
		// 因为这个是 TimeEvictor，是需要根据 windowSize 和当前 elements 中最大的 ts 来决定
		// 驱逐哪些元素的
		if (!hasTimestamp(elements)) {
			return;
		}

		long currentTime = getMaxTimestamp(elements);
		long evictCutoff = currentTime - windowSize;

		// currentTime 是元素集中时间戳最大的，windowSize 是窗口的大小
		// evictCutoff 是能够存活的最低的界限
		for (Iterator<TimestampedValue<Object>> iterator = elements.iterator(); iterator.hasNext(); ) {
			TimestampedValue<Object> record = iterator.next();
			if (record.getTimestamp() <= evictCutoff) {
				iterator.remove();
			}
		}
	}
	```

* DeltaEvictor

	DeltaEvictor 通过 deltaFunction 计算每个元素与最后一个元素的差值，当差值大于 threshold 的时候，remove 该元素

	```java
	DeltaFunction<T> deltaFunction;  // 计算两个元素的差值的函数
	private double threshold;  // 窗口能接受最大的差值
	private final boolean doEvictAfter; // 在窗口函数执行前还是后执行驱逐操作
	
	// Evictor 抽象类的 evictBefore 和 evictAfter 方法都会调用 evict 方法
	private void evict(Iterable<TimestampedValue<T>> elements, int size, EvictorContext ctx) {
		// 以 elements 中最后一个元素作为标杆，当 elements 中的元素与最后一个元素的 delta 数值
		// 高于限制，remove 该元素
		TimestampedValue<T> lastElement = Iterables.getLast(elements);
		for (Iterator<TimestampedValue<T>> iterator = elements.iterator(); iterator.hasNext();){
			TimestampedValue<T> element = iterator.next();
			if (deltaFunction.getDelta(element.getValue(), lastElement.getValue()) >= this.threshold) {
				iterator.remove();
			}
		}
	}
	```

* CountEvictor

	CountEvictor 指代窗口最多能够保留 maxCount 数量的元素，如果元素的数量大于 maxCount，会从元素集合的开头开始 remove 元素，直到元素的数量等于 maxCount

	```java
	private final long maxCount;  // 最多保留多少数量的 record
	private final boolean doEvictAfter;  // 在窗口函数执行前还是后执行驱逐操作
	
	// 相当于一个滑动窗口，当大小超出 maxCount 的时候，从迭代器的开端 remove record
	private void evict(Iterable<TimestampedValue<Object>> elements, int size, EvictorContext ctx) {
		if (size <= maxCount) {
			return;
		} else {
			int evictedCount = 0;
			for (Iterator<TimestampedValue<Object>> iterator = elements.iterator(); iterator.hasNext();){
				iterator.next();
				evictedCount++;
				if (evictedCount > size - maxCount) {
					// 当驱逐数量够了的时候，break
					break;
				} else {
					iterator.remove();
				}
			}
		}
	}
	```
	
## 总结

这篇文章给大家讲解了一下几个非常重要的类，在之后的文章中，大家可以看到，在窗口操作中，这些类扮演了非常重要的角色


