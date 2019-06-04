# flink 的窗口 —— 窗口操作符

这篇文章我们来讲一下 flink 的窗口操作符。首先我们来回忆一下上一篇文章中 flink 的一些组件类，WindowAssigner 是窗口分配器，用于给一个元素分配一个或多个窗口，Trigger 是一个触发器，用来决定何时计算窗口 emit 该窗口部分的结果，Evictor 用于在窗口函数执行前后删除部分窗格内的元素，此外，窗口操作重度依赖 flink 的定时器，有所遗忘的同学可以结合[flink 中的定时器](./docs/flink-timer.md)进行复习

窗口操作符根据是否设置 Evictor 分为 WindowOperator 和 EvictingWindowOperator，EvictingWindowOperator 继承 WindowOperator 类，在 WindowOperator 的 emitWindowContents 方法上增加了 Evictor 相关的处理，我们先来看看 WindowOperator 的源码

## WindowOperator

### WindowOperator 的属性

```java
// 窗口分配器
protected final WindowAssigner<? super IN, W> windowAssigner;
	
// key 选择器
private final KeySelector<IN, K> keySelector;
	
// 触发器
private final Trigger<? super IN, ? super W> trigger;
	
// 窗口状态描述器
private final StateDescriptor<? extends AppendingState<IN, ACC>, ?> windowStateDescriptor;
	
// key 序列器
protected final TypeSerializer<K> keySerializer;
	
// 窗口序列器，每个窗口都是一个命名空间，windowSerializer 用来创建 InternalTimerService
protected final TypeSerializer<W> windowSerializer;
	
// 窗口允许元素延迟 allowedLateness 到达
protected final long allowedLateness;
	
// 侧边输出 outputTag，可以用于处理晚到的元素
protected final OutputTag<IN> lateDataOutputTag;
	
// 存储窗口内容的状态。每个窗口都是一个命名空间
private transient InternalAppendingState<K, W, IN, ACC, ACC> windowState;
	
// 在 InternalWindowFunction 中用于输出给定 ts 的元素
protected transient TimestampedCollector<OUT> timestampedCollector;
	
// 给 trigger 使用的上下文
protected transient Context triggerContext = new Context(null, null);
	
// 给窗口函数使用的上下文
protected transient WindowContext processContext;
	
// 获取当前的进程时间
protected transient WindowAssigner.WindowAssignerContext windowAssignerContext;
	
// 内部时间定时器
protected transient InternalTimerService<W> internalTimerService;
```

### processElement

窗口操作符继承 AbstractStreamOperator 类，因此使用 processElement 方法来处理流入的流元素

首先，使用 windowAssigner 为元素分配窗口，定义 isSkippedElement 指代元素没有被任何一个窗口处理（为元素分配的每个窗口都过期了-。-）

```java
// 得到元素属于的窗口集合
final Collection<W> elementWindows = windowAssigner.assignWindows(
	element.getValue(), element.getTimestamp(), windowAssignerContext);

// 如果元素没有被任何一个窗口处理
boolean isSkippedElement = true;

final K key = this.<K>getKeyedStateBackend().getCurrentKey();
```

接下来，需要判断 windowAssigner 是否为 MergingWindowAssigner

* windowAssigner 不为 MergingWindowAssigner
	
	```java
	// 遍历元素所属的所有窗口
	for (W window: elementWindows) {

		// 如果事件时间窗口右端 + allowedLateness <= Watermark，直接忽略
		if (isWindowLate(window)) {
			continue;
		}
		// 窗口是有效的，说明元素被处理了，isSkippedElement 设置为 false
		isSkippedElement = false;

		// 每一个窗口都是一个命名空间
		// 先设置 windowState 的命名空间为当前 window
		// 然后添加当前 element
		windowState.setCurrentNamespace(window);
		windowState.add(element.getValue());

		// 设置 triggerContext 当前的 key 和 window
		triggerContext.key = key;
		triggerContext.window = window;

		TriggerResult triggerResult = triggerContext.onElement(element);
		
		// 本元素触发了窗口的执行
		if (triggerResult.isFire()) {
			ACC contents = windowState.get();
			if (contents == null) {
				continue;
			}
			// 触发器被触发的时候，emit 窗口的内容
			emitWindowContents(window, contents);
		}

		if (triggerResult.isPurge()) {
			windowState.clear();  // 清理窗口元信息
		}
		registerCleanupTimer(window);  // 用窗口的末端注册定时器
	}
	```

* windowAssigner 为 MergingWindowAssigner

	MergingWindowAssigner 的处理就复杂了很多，MergingWindowAssigner 能够合并窗口，ProcessingTimeSessionWindows、EventTimeSessionWindows、DynamicProcessingTimeSessionWindows 以及 DynamicEventTimeSessionWindows 继承了 MergingWindowAssigner，它们都是依赖 sessionTimeout 来分配窗口的，完全可以合并
	
	首先，我们来介绍一下 MergingWindowSet —— 窗口合并的工具，MergingWindowSet 用于保存窗口和命名空间窗口的映射，合并给出的窗口集合，在窗口合并后进行命名空间合并以及状态合并
	
	* MergingWindowSet 的属性和构造函数

		state 是一个元素为 Tuple2<W, W> 的 ListState，Tuple2.f0 为真实窗口，Tuple2.f1 为命名空间窗口
		
		```java
		public class MergingWindowSet<W extends Window> {
			// 用于保存窗口到其命名窗口的映射
			private final Map<W, W> mapping;
			// 构造函数中根据传入的 state 生成的初始映射
			// persist 中用于和 mapping 比较，判断是否需要更新 state
			private final Map<W, W> initialMapping;
			
			// 状态
			private final ListState<Tuple2<W, W>> state;
			// 合并窗口分配器
			private final MergingWindowAssigner<?, W> windowAssigner;
			
			/**
			 * 从给定的 state 中恢复一个 MergingWindowSet
			 */
			public MergingWindowSet(MergingWindowAssigner<?, W> windowAssigner, ListState<Tuple2<W, W>> state) throws Exception {
				this.windowAssigner = windowAssigner;
				mapping = new HashMap<>();
		
				// 从 state 中获取 Tuple2 的集合
				Iterable<Tuple2<W, W>> windowState = state.get();
				if (windowState != null) {
					for (Tuple2<W, W> window: windowState) {
						mapping.put(window.f0, window.f1);
					}
				}
		
				this.state = state;
		
				initialMapping = new HashMap<>();
				initialMapping.putAll(mapping);
			}
		}
		```
	
	* MergingWindowSet 的 persist 方法

		通过比较 mapping 和 initialMapping 是否相等，判断是否需要重写 state

		```java
		/**
		 * 如果映射自初始化后发生更改，则将更新的映射保留到给定状态
		 */
		public void persist() throws Exception {
			if (!mapping.equals(initialMapping)) {
				state.clear();
				for (Map.Entry<W, W> window : mapping.entrySet()) {
					state.add(new Tuple2<>(window.getKey(), window.getValue()));
				}
			}
		}
		```
	
	* MergingWindowSet 的 getStateWindow 和 retireWindow 方法
		
		getStateWindow 从 mapping 中获取 window 对应的命名空间窗口，retireWindow 将 window 为 key 的 kv 对从 mapping 中删除
		
		```java
		public W getStateWindow(W window) {
			return mapping.get(window);
		}
		
		public void retireWindow(W window) {
			W removed = this.mapping.remove(window);
			if (removed == null) {
				throw new IllegalStateException("Window " + window + " is not in in-flight window set.");
			}
		}
		```
	
	* MergingWindowSet 的 addWindow 方法

		addWindow 用于将新的窗口和现有的所有窗口进行合并，我们前面提到过 mapping 是窗口和命名空间窗口的映射。我们调用 windowAssigner 的 mergeWindows 方法，mergeResults 中就是合并结果窗口与参与合并的窗口集合的映射，我们选取集合中的第一个窗口对应的命名空间窗口作为合并结果的命名空间窗口，并将集合中的窗口全部从 mapping 中删除，最后调用 mergeFunction.merge 来合并命名空间和状态
		
		```java
		public W addWindow(W newWindow, MergeFunction<W> mergeFunction) throws Exception {
		
			List<W> windows = new ArrayList<>();
			// 将当前 mapping 中的所有 key 窗口和新的窗口加入 windows
			windows.addAll(this.mapping.keySet());
			windows.add(newWindow);
	
			// mergeResults，k 是合并后的窗口，v 是合并后的窗口包含的原始窗口集合
			// windowAssigner 的 mergeWindows 方法会调用 TimeWindow 的 mergeWindows 方法
			final Map<W, Collection<W>> mergeResults = new HashMap<>();
			windowAssigner.mergeWindows(windows,
					new MergingWindowAssigner.MergeCallback<W>() {
						@Override
						public void merge(Collection<W> toBeMerged, W mergeResult) {
							if (LOG.isDebugEnabled()) {
								LOG.debug("Merging {} into {}", toBeMerged, mergeResult);
							}
							mergeResults.put(mergeResult, toBeMerged);
						}
					});
	
			W resultWindow = newWindow;
			boolean mergedNewWindow = false;  // 标志新窗口是否被合并
	
			// perform the merge
			for (Map.Entry<W, Collection<W>> c: mergeResults.entrySet()) {
				W mergeResult = c.getKey();
				Collection<W> mergedWindows = c.getValue();
	
				// 如果新窗口位于合并窗口中，则将合并结果设为结果窗口
				// 这里也感觉可以优化，因为 TimeWindow 的 mergeWindows 方法实现
				// 保证一个 window 仅仅存在于一个 mergedWindows 中
				if (mergedWindows.remove(newWindow)) {
					mergedNewWindow = true;
					resultWindow = mergeResult;
				}
	
				// 选择任意一个窗口，并选择该窗口的状态窗口作为合并结果的状态窗口
				W mergedStateWindow = this.mapping.get(mergedWindows.iterator().next());

				// 把当前 mapping 中的 key 存在于 mergedWindows 中的 (k, v) 全部删除
				List<W> mergedStateWindows = new ArrayList<>();
				for (W mergedWindow: mergedWindows) {
					W res = this.mapping.remove(mergedWindow);
					if (res != null) {
						mergedStateWindows.add(res);  // 合并集合中所有 k 的 v 组成的集合
					}
				}
	
				this.mapping.put(mergeResult, mergedStateWindow);
	
				// 需要把我们用来做合并结果状态的窗口删除
				mergedStateWindows.remove(mergedStateWindow);

				// 不要合并新的窗口自己，它没有任何与其相关的状态
				if (!(mergedWindows.contains(mergeResult) && mergedWindows.size() == 1)) {
					mergeFunction.merge(mergeResult,
							mergedWindows,
							this.mapping.get(mergeResult),
							mergedStateWindows);
				}
			}

			// 窗口没有被 merge，成为了一个新的窗口
			if (mergeResults.isEmpty() || (resultWindow.equals(newWindow) && !mergedNewWindow)) {
				this.mapping.put(resultWindow, resultWindow);
			}
	
			return resultWindow;
		}
		```

	在了解完 MergingWindowSet 之后，再来看看 processElement 的逻辑，可以看到 addWindow 方法中传入了一个 MergingWindowSet.MergeFunction 实例，会将被合并的窗口销毁，同时合并窗口状态和命名空间。剩下的代码逻辑和 windowAssigner 不为 MergingWindowAssigner 类似，只需要注意这里使用 MergingWindowSet.mapping 中 actualWindow 对应 value 作为命名空间
	
	```java
	MergingWindowSet<W> mergingWindows = getMergingWindowSet();

	for (W window: elementWindows) {
		// 添加新的窗口有可能导致一次合并，这种情况下真正的窗口是合并后的窗口，同时我们也工作在合并后的窗口上
		// 如果新的窗口没有 merge，actualWindow 就等于 window
		W actualWindow = mergingWindows.addWindow(window, new MergingWindowSet.MergeFunction<W>() {
			@Override
			public void merge(W mergeResult,
					Collection<W> mergedWindows, W stateWindowResult,
					Collection<W> mergedStateWindows) throws Exception {
				triggerContext.key = key;
				triggerContext.window = mergeResult;

				triggerContext.onMerge(mergedWindows);

				// 被合并的窗口被销毁了
				for (W m: mergedWindows) {
					triggerContext.window = m;
					triggerContext.clear();
					deleteCleanupTimer(m);
				}

				// 将被合并的状态窗口合并到新生成的状态窗口
				windowMergingState.mergeNamespaces(stateWindowResult, mergedStateWindows);
			}
		});

		// 如果合并得到的结果窗口已经延迟了，删除给定窗口
		if (isWindowLate(actualWindow)) {
			mergingWindows.retireWindow(actualWindow);
			continue;
		}
		isSkippedElement = false;

		W stateWindow = mergingWindows.getStateWindow(actualWindow);
		if (stateWindow == null) {
			throw new IllegalStateException("Window " + window + " is not in in-flight window set.");
		}

		// 合并分配器的命名空间是 stateWindow 也就是 mergingWindows 中 mapping 的 value
		// 先设置 windowState 的命名空间为当前 stateWindow
		// 然后添加当前 element
		windowState.setCurrentNamespace(stateWindow);
		windowState.add(element.getValue());

		triggerContext.key = key;
		triggerContext.window = actualWindow;

		TriggerResult triggerResult = triggerContext.onElement(element);

		if (triggerResult.isFire()) {
			ACC contents = windowState.get();
			if (contents == null) {
				continue;
			}
			emitWindowContents(actualWindow, contents);
		}

		if (triggerResult.isPurge()) {
			windowState.clear();
		}
		registerCleanupTimer(actualWindow);
	}

	// need to make sure to update the merging state in state
	// 需要确保更新合并后的状态
	mergingWindows.persist();
	```

最后，如果 isSkippedElement 为 true 的话，说明元素没有被任何一个窗口使用，可以使用 sideOutput 进行侧边输出

```java
// 可以用侧边输出来处理延迟到达的元素
if (isSkippedElement && isElementLate(element)) {
	if (lateDataOutputTag != null){
		sideOutput(element);
	} else {
		this.numLateRecordsDropped.inc();
	}
}
```

### onProcessingTime

onProcessingTime 在进程时间定时器触发的时候被调用，首先，从 timer 总获取 key 和 window（用作命名空间），然后设置 windowState 的命名空间，从中获取窗口的内容，再调用 emitWindowContents 来 emit 窗口内容，最后，清空窗口的各种状态

```java
public void onProcessingTime(InternalTimer<K, W> timer) throws Exception {
	triggerContext.key = timer.getKey();
	triggerContext.window = timer.getNamespace();

	MergingWindowSet<W> mergingWindows;

	if (windowAssigner instanceof MergingWindowAssigner) {
		mergingWindows = getMergingWindowSet();
		W stateWindow = mergingWindows.getStateWindow(triggerContext.window);
		if (stateWindow == null) {
			return;
		} else {
			windowState.setCurrentNamespace(stateWindow);
		}
	} else {
		windowState.setCurrentNamespace(triggerContext.window);
		mergingWindows = null;
	}

	TriggerResult triggerResult = triggerContext.onProcessingTime(timer.getTimestamp());

	if (triggerResult.isFire()) {
		ACC contents = windowState.get();
		if (contents != null) {
			emitWindowContents(triggerContext.window, contents);
		}
	}

	if (triggerResult.isPurge()) {
		windowState.clear();
	}

	if (!windowAssigner.isEventTime() && isCleanupTime(triggerContext.window, timer.getTimestamp())) {
		clearAllState(triggerContext.window, windowState, mergingWindows);
	}

	// 我感觉这步是多余的，因为唯一可能发送变化的 clearAllState 方法也执行了 persist 方法
	if (mergingWindows != null) {
		mergingWindows.persist();
	}
}
```

### onEventTime

onEventTime 在事件时间定时器触发的时候被调用，onEventTime 和 onProcessingTime 代码基本相同，除了在调用 clearAllState 的时候，判断不同，如下所示

```java
if (windowAssigner.isEventTime() && isCleanupTime(triggerContext.window, timer.getTimestamp())) {
	clearAllState(triggerContext.window, windowState, mergingWindows);
}
```

### emitWindowContents

执行 InternalWindowFunction 的 process 方法，将窗口的内容处理后输出到下游，需要注意，输出的 StreamRecord 的时间戳都是窗口的右边界

```java
/**
 * 使用 InternalWindowFunction 发出给定窗口的内容
 * 窗口触发的时候，output 的 StreamRecord 的 ts 都是窗口的右边界
 */
private void emitWindowContents(W window, ACC contents) throws Exception {
	timestampedCollector.setAbsoluteTimestamp(window.maxTimestamp());
	processContext.window = window;
	userFunction.process(triggerContext.key, window, processContext, contents, timestampedCollector);
}
```

## EvictingWindowOperator

EvictingWindowOperator 会在 emitWindowContents 的 `userFunction.process` 前后执行驱逐者的逻辑，用来清除集合中的部分元素，这里直接看 emitWindowContents 方法

首先，我们会得到一个 recordsWithTimestamp，这是一个 `TimestampedValue<IN>` 组成的集合，TimestampedValue 和 StreamRecord 类似，TimestampedValue 只保存了 StreamRecord 的 value 和 timestamp（这两个值在 Evitor 中被使用），然后调用 `evictorContext.evictBefore` 进行驱逐操作

evictBefore 执行完之后，我们再将 TimestampedValue 转换为 StreamRecord，执行 `userFunction.process`

最后，我们对 recordsWithTimestamp 执行 `evictorContext.evictAfter` 操作，清空 WindowState，再将 recordsWithTimestamp 剩余的元素写入 WindowState

```java
private void emitWindowContents(W window, Iterable<StreamRecord<IN>> contents, ListState<StreamRecord<IN>> windowState) throws Exception {
	// 设置指定时间戳
	timestampedCollector.setAbsoluteTimestamp(window.maxTimestamp());

	// 将 contents 的内容由 StreamRecord 转为 TimestampedValue
	FluentIterable<TimestampedValue<IN>> recordsWithTimestamp = FluentIterable
		.from(contents)
		.transform(new Function<StreamRecord<IN>, TimestampedValue<IN>>() {
			@Override
			public TimestampedValue<IN> apply(StreamRecord<IN> input) {
				return TimestampedValue.from(input);
			}
		});
	evictorContext.evictBefore(recordsWithTimestamp, Iterables.size(recordsWithTimestamp));

	// userFunction 只处理 record 的 value，这里再转换一次
	FluentIterable<IN> projectedContents = recordsWithTimestamp
		.transform(new Function<TimestampedValue<IN>, IN>() {
			@Override
			public IN apply(TimestampedValue<IN> input) {
				return input.getValue();
			}
		});

	processContext.window = triggerContext.window;
	userFunction.process(triggerContext.key, triggerContext.window, processContext, projectedContents, timestampedCollector);
	evictorContext.evictAfter(recordsWithTimestamp, Iterables.size(recordsWithTimestamp));

	// clear 掉被驱逐的元素的 state，先 clear 再 add 不是有效的做法，但是也没有其他的方法
	windowState.clear();
	for (TimestampedValue<IN> record : recordsWithTimestamp) {
		windowState.add(record.getStreamRecord());
	}
}
```

## 总结

今天我们讲解了一下窗口操作符，窗口操作符继承了 AbstractStreamOperator，通过 processElement 方法处理到来的元素，操作符会给元素分配窗口，当窗口触发的时候，调用 emitWindowContents 方法将窗口中的内容 emit 到下游，希望对大家有所帮助