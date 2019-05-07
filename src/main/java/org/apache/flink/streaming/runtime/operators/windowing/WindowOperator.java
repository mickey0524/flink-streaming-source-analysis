/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.operators.windowing;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.AppendingState;
import org.apache.flink.api.common.state.FoldingState;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.MergingState;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.DefaultKeyedStateStore;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.internal.InternalAppendingState;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.runtime.state.internal.InternalMergingState;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.windowing.assigners.BaseAlignedWindowAssigner;
import org.apache.flink.streaming.api.windowing.assigners.MergingWindowAssigner;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalWindowFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;

import java.io.Serializable;
import java.util.Collection;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An operator that implements the logic for windowing based on a {@link WindowAssigner} and
 * {@link Trigger}.
 *
 * 一个依据 WindowAssigner 和 Trigger 实现了窗口逻辑的操作符
 *
 * <p>When an element arrives it gets assigned a key using a {@link KeySelector} and it gets
 * assigned to zero or more windows using a {@link WindowAssigner}. Based on this, the element
 * is put into panes. A pane is the bucket of elements that have the same key and same
 * {@code Window}. An element can be in multiple panes if it was assigned to multiple windows by the
 * {@code WindowAssigner}.
 *
 * 当一个元素到来，使用 KeySelector 获得获得一个 key，并且，元素通过 WindowAssigner 分配一个或多个窗口
 * 依靠这个，元素被放进窗格。一个窗格是一个拥有相同 key 和相同窗口的元素组成的桶，一个元素能够位于多个窗格中
 * 因为一个元素能够被分配多个窗口
 *
 * <p>Each pane gets its own instance of the provided {@code Trigger}. This trigger determines when
 * the contents of the pane should be processed to emit results. When a trigger fires,
 * the given {@link InternalWindowFunction} is invoked to produce the results that are emitted for
 * the pane to which the {@code Trigger} belongs.
 *
 * 每个窗格拥有自己的 Trigger 实例，触发器决定了什么时候窗格的内容被处理来输出结果
 * 当 trigger 被触发，InternalWindowFunction 被调用来输出结果
 *
 * @param <K> The type of key returned by the {@code KeySelector}.
 * @param <IN> The type of the incoming elements.
 * @param <OUT> The type of elements emitted by the {@code InternalWindowFunction}.
 * @param <W> The type of {@code Window} that the {@code WindowAssigner} assigns.
 */
@Internal
public class WindowOperator<K, IN, ACC, OUT, W extends Window>
	extends AbstractUdfStreamOperator<OUT, InternalWindowFunction<ACC, OUT, K, W>>
	implements OneInputStreamOperator<IN, OUT>, Triggerable<K, W> {

	private static final long serialVersionUID = 1L;

	// ------------------------------------------------------------------------
	// Configuration values and user functions
	// ------------------------------------------------------------------------

	protected final WindowAssigner<? super IN, W> windowAssigner;  // 窗口分配器

	private final KeySelector<IN, K> keySelector;  // key 选择器

	private final Trigger<? super IN, ? super W> trigger; // 触发器

	private final StateDescriptor<? extends AppendingState<IN, ACC>, ?> windowStateDescriptor;  // 窗口状态描述器

	/** For serializing the key in checkpoints. */
	protected final TypeSerializer<K> keySerializer;  // key 序列器

	/** For serializing the window in checkpoints. */
	protected final TypeSerializer<W> windowSerializer;  // 窗口序列器

	/**
	 * The allowed lateness for elements. This is used for:
	 * <ul>
	 *     <li>Deciding if an element should be dropped from a window due to lateness.
	 *     <li>Clearing the state of a window if the system time passes the
	 *         {@code window.maxTimestamp + allowedLateness} landmark.
	 * </ul>
	 */
	protected final long allowedLateness;  // 允许延迟

	/**
	 * {@link OutputTag} to use for late arriving events. Elements for which
	 * {@code window.maxTimestamp + allowedLateness} is smaller than the current watermark will
	 * be emitted to this.
	 */
	protected final OutputTag<IN> lateDataOutputTag;  // 侧边输出 outputTag

	private static final String LATE_ELEMENTS_DROPPED_METRIC_NAME = "numLateRecordsDropped";

	protected transient Counter numLateRecordsDropped;

	// ------------------------------------------------------------------------
	// State that is not checkpointed
	// ------------------------------------------------------------------------

	/** The state in which the window contents is stored. Each window is a namespace */
	// 存储窗口内容的状态。每个窗口都是一个命名空间
	private transient InternalAppendingState<K, W, IN, ACC, ACC> windowState;

	/**
	 * The {@link #windowState}, typed to merging state for merging windows.
	 * Null if the window state is not mergeable.
	 */
	// 合并窗口的状态
	private transient InternalMergingState<K, W, IN, ACC, ACC> windowMergingState;

	/** The state that holds the merging window metadata (the sets that describe what is merged). */
	// 合并窗口元信息
	private transient InternalListState<K, VoidNamespace, Tuple2<W, W>> mergingSetsState;

	/**
	 * This is given to the {@code InternalWindowFunction} for emitting elements with a given
	 * timestamp.
	 */
	// 在 InternalWindowFunction 中用于输出给定 ts 的元素
	protected transient TimestampedCollector<OUT> timestampedCollector;

	protected transient Context triggerContext = new Context(null, null);

	protected transient WindowContext processContext;

	// 获取当前的进程时间
	protected transient WindowAssigner.WindowAssignerContext windowAssignerContext;

	// ------------------------------------------------------------------------
	// State that needs to be checkpointed
	// ------------------------------------------------------------------------

	protected transient InternalTimerService<W> internalTimerService;

	/**
	 * Creates a new {@code WindowOperator} based on the given policies and user functions.
	 */
	public WindowOperator(
			WindowAssigner<? super IN, W> windowAssigner,
			TypeSerializer<W> windowSerializer,
			KeySelector<IN, K> keySelector,
			TypeSerializer<K> keySerializer,
			StateDescriptor<? extends AppendingState<IN, ACC>, ?> windowStateDescriptor,
			InternalWindowFunction<ACC, OUT, K, W> windowFunction,
			Trigger<? super IN, ? super W> trigger,
			long allowedLateness,
			OutputTag<IN> lateDataOutputTag) {

		super(windowFunction);  // 设置 userFunction

		checkArgument(!(windowAssigner instanceof BaseAlignedWindowAssigner),
			"The " + windowAssigner.getClass().getSimpleName() + " cannot be used with a WindowOperator. " +
				"This assigner is only used with the AccumulatingProcessingTimeWindowOperator and " +
				"the AggregatingProcessingTimeWindowOperator");

		checkArgument(allowedLateness >= 0);

		checkArgument(windowStateDescriptor == null || windowStateDescriptor.isSerializerInitialized(),
				"window state serializer is not properly initialized");

		this.windowAssigner = checkNotNull(windowAssigner);
		this.windowSerializer = checkNotNull(windowSerializer);
		this.keySelector = checkNotNull(keySelector);
		this.keySerializer = checkNotNull(keySerializer);
		this.windowStateDescriptor = windowStateDescriptor;
		this.trigger = checkNotNull(trigger);
		this.allowedLateness = allowedLateness;
		this.lateDataOutputTag = lateDataOutputTag;

		setChainingStrategy(ChainingStrategy.ALWAYS);
	}

	@Override
	public void open() throws Exception {
		super.open();

		this.numLateRecordsDropped = metrics.counter(LATE_ELEMENTS_DROPPED_METRIC_NAME);
		timestampedCollector = new TimestampedCollector<>(output);

		internalTimerService =
				getInternalTimerService("window-timers", windowSerializer, this);

		triggerContext = new Context(null, null);
		processContext = new WindowContext(null);

		windowAssignerContext = new WindowAssigner.WindowAssignerContext() {
			@Override
			public long getCurrentProcessingTime() {
				return internalTimerService.currentProcessingTime();
			}
		};

		// create (or restore) the state that hold the actual window contents
		// NOTE - the state may be null in the case of the overriding evicting window operator
		// 创建或者恢复状态，状态 hold 了真正的窗口内容
		if (windowStateDescriptor != null) {
			windowState = (InternalAppendingState<K, W, IN, ACC, ACC>) getOrCreateKeyedState(windowSerializer, windowStateDescriptor);
		}

		// create the typed and helper states for merging windows
		// 创建用于合并窗口的类型化状态和帮助状态
		if (windowAssigner instanceof MergingWindowAssigner) {

			// store a typed reference for the state of merging windows - sanity check
			if (windowState instanceof InternalMergingState) {
				windowMergingState = (InternalMergingState<K, W, IN, ACC, ACC>) windowState;
			}
			// TODO this sanity check should be here, but is prevented by an incorrect test (pending validation)
			// TODO see WindowOperatorTest.testCleanupTimerWithEmptyFoldingStateForSessionWindows()
			// TODO activate the sanity check once resolved
//			else if (windowState != null) {
//				throw new IllegalStateException(
//						"The window uses a merging assigner, but the window state is not mergeable.");
//			}

			@SuppressWarnings("unchecked")
			final Class<Tuple2<W, W>> typedTuple = (Class<Tuple2<W, W>>) (Class<?>) Tuple2.class;

			final TupleSerializer<Tuple2<W, W>> tupleSerializer = new TupleSerializer<>(
					typedTuple,
					new TypeSerializer[] {windowSerializer, windowSerializer});

			final ListStateDescriptor<Tuple2<W, W>> mergingSetsStateDescriptor =
					new ListStateDescriptor<>("merging-window-set", tupleSerializer);

			// get the state that stores the merging sets
			mergingSetsState = (InternalListState<K, VoidNamespace, Tuple2<W, W>>)
					getOrCreateKeyedState(VoidNamespaceSerializer.INSTANCE, mergingSetsStateDescriptor);
			mergingSetsState.setCurrentNamespace(VoidNamespace.INSTANCE);
		}
	}

	@Override
	public void close() throws Exception {
		super.close();
		timestampedCollector = null;
		triggerContext = null;
		processContext = null;
		windowAssignerContext = null;
	}

	@Override
	public void dispose() throws Exception {
		super.dispose();
		timestampedCollector = null;
		triggerContext = null;
		processContext = null;
		windowAssignerContext = null;
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		// 得到元素属于的窗口集合
		final Collection<W> elementWindows = windowAssigner.assignWindows(
			element.getValue(), element.getTimestamp(), windowAssignerContext);

		//if element is handled by none of assigned 
		// 如果元素没有被任何一个窗口处理
		boolean isSkippedElement = true;

		final K key = this.<K>getKeyedStateBackend().getCurrentKey();

		// 当窗口分配器是合并窗口分配器的时候
		if (windowAssigner instanceof MergingWindowAssigner) {
			MergingWindowSet<W> mergingWindows = getMergingWindowSet();

			for (W window: elementWindows) {

				// adding the new window might result in a merge, in that case the actualWindow
				// is the merged window and we work with that. If we don't merge then
				// actualWindow == window
				// 添加新的窗口有可能导致一次合并，这种情况下真正的窗口是合并后的窗口，同时我们也工作在合并后的窗口上
				// 如果新的窗口没有 merge，actualWindow 就等于 window
				W actualWindow = mergingWindows.addWindow(window, new MergingWindowSet.MergeFunction<W>() {
					@Override
					public void merge(W mergeResult,
							Collection<W> mergedWindows, W stateWindowResult,
							Collection<W> mergedStateWindows) throws Exception {
						
						// 异常检查，判断合并后的窗口是否满足要求
						if ((windowAssigner.isEventTime() && mergeResult.maxTimestamp() + allowedLateness <= internalTimerService.currentWatermark())) {
							throw new UnsupportedOperationException("The end timestamp of an " +
									"event-time window cannot become earlier than the current watermark " +
									"by merging. Current watermark: " + internalTimerService.currentWatermark() +
									" window: " + mergeResult);
						} else if (!windowAssigner.isEventTime() && mergeResult.maxTimestamp() <= internalTimerService.currentProcessingTime()) {
							throw new UnsupportedOperationException("The end timestamp of a " +
									"processing-time window cannot become earlier than the current processing time " +
									"by merging. Current processing time: " + internalTimerService.currentProcessingTime() +
									" window: " + mergeResult);
						}

						triggerContext.key = key;
						triggerContext.window = mergeResult;

						triggerContext.onMerge(mergedWindows);

						// 被合并的窗口被销毁了
						for (W m: mergedWindows) {
							triggerContext.window = m;
							triggerContext.clear();
							deleteCleanupTimer(m);
						}

						// merge the merged state windows into the newly resulting state window
						// 将被合并的状态窗口合并到新生成的状态窗口
						windowMergingState.mergeNamespaces(stateWindowResult, mergedStateWindows);
					}
				});

				// drop if the window is already late
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
		} else {
			// 便利元素所属的所有窗口
			for (W window: elementWindows) {

				// drop if the window is already late
				// 如果元素大于延迟最大范围，直接忽略
				if (isWindowLate(window)) {
					continue;
				}
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
				registerCleanupTimer(window);  // 当窗口过期了清理窗口内容
			}
		}

		// side output input event if
		// element not handled by any window
		// late arriving tag has been set
		// windowAssigner is event time and current timestamp + allowed lateness no less than element timestamp
		// 可以用侧边输出来处理延迟到达的元素
		if (isSkippedElement && isElementLate(element)) {
			if (lateDataOutputTag != null){
				sideOutput(element);
			} else {
				this.numLateRecordsDropped.inc();
			}
		}
	}

	@Override
	/**
	 * 事件时间定时器触发调用
	 */
	public void onEventTime(InternalTimer<K, W> timer) throws Exception {
		triggerContext.key = timer.getKey();
		triggerContext.window = timer.getNamespace();

		MergingWindowSet<W> mergingWindows;

		if (windowAssigner instanceof MergingWindowAssigner) {
			mergingWindows = getMergingWindowSet();
			W stateWindow = mergingWindows.getStateWindow(triggerContext.window);
			if (stateWindow == null) {
				// Timer firing for non-existent window, this can only happen if a
				// trigger did not clean up timers. We have already cleared the merging
				// window and therefore the Trigger state, however, so nothing to do.
				// 计时器触发不存在的窗口，这只能在触发器没有清除干净定时器的时候发生
				// 我们已经清除了合并窗口，因此触发状态，但是，没有什么要做的
				return;
			} else {
				windowState.setCurrentNamespace(stateWindow);
			}
		} else {
			windowState.setCurrentNamespace(triggerContext.window);
			mergingWindows = null;
		}

		TriggerResult triggerResult = triggerContext.onEventTime(timer.getTimestamp());

		if (triggerResult.isFire()) {
			ACC contents = windowState.get();
			if (contents != null) {
				emitWindowContents(triggerContext.window, contents);
			}
		}

		if (triggerResult.isPurge()) {
			windowState.clear();
		}

		if (windowAssigner.isEventTime() && isCleanupTime(triggerContext.window, timer.getTimestamp())) {
			clearAllState(triggerContext.window, windowState, mergingWindows);
		}

		// 我感觉这步是多余的，因为唯一可能发送变化的 clearAllState 方法也执行了 persist 方法
		if (mergingWindows != null) {
			// need to make sure to update the merging state in state
			mergingWindows.persist();
		}
	}

	@Override
	/**
	 * 进程时间定时器触发调用
	 */
	public void onProcessingTime(InternalTimer<K, W> timer) throws Exception {
		triggerContext.key = timer.getKey();
		triggerContext.window = timer.getNamespace();

		MergingWindowSet<W> mergingWindows;

		if (windowAssigner instanceof MergingWindowAssigner) {
			mergingWindows = getMergingWindowSet();
			W stateWindow = mergingWindows.getStateWindow(triggerContext.window);
			if (stateWindow == null) {
				// Timer firing for non-existent window, this can only happen if a
				// trigger did not clean up timers. We have already cleared the merging
				// window and therefore the Trigger state, however, so nothing to do.
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

		// 我感觉这步是多余的
		if (mergingWindows != null) {
			// need to make sure to update the merging state in state
			mergingWindows.persist();
		}
	}

	/**
	 * Drops all state for the given window and calls
	 * {@link Trigger#clear(Window, Trigger.TriggerContext)}.
	 *
	 * <p>The caller must ensure that the
	 * correct key is set in the state backend and the triggerContext object.
	 */
	/**
	 * 删除给定窗口的所有状态同时调用
	 * trigger 的 clear 
	 *
	 * 必须确保正确的 key 被设置在 windowState 和 triggerContext 中
	 * windowState 设置了 namespace
	 * triggerContext 设置了 key 和 window
	 */
	private void clearAllState(
			W window,
			AppendingState<IN, ACC> windowState,
			MergingWindowSet<W> mergingWindows) throws Exception {
		windowState.clear();
		triggerContext.clear();
		processContext.window = window;
		processContext.clear();
		if (mergingWindows != null) {
			mergingWindows.retireWindow(window);
			mergingWindows.persist();
		}
	}

	/**
	 * Emits the contents of the given window using the {@link InternalWindowFunction}.
	 */
	/**
	 * 使用 InternalWindowFunction 发出给定窗口的内容
	 */
	@SuppressWarnings("unchecked")
	private void emitWindowContents(W window, ACC contents) throws Exception {
		timestampedCollector.setAbsoluteTimestamp(window.maxTimestamp());
		processContext.window = window;
		userFunction.process(triggerContext.key, window, processContext, contents, timestampedCollector);
	}

	/**
	 * Write skipped late arriving element to SideOutput.
	 *
	 * @param element skipped late arriving element to side output
	 */
	/**
	 * 将延迟到达的元素写入侧边输出
	 */
	protected void sideOutput(StreamRecord<IN> element){
		output.collect(lateDataOutputTag, element);
	}

	/**
	 * Retrieves the {@link MergingWindowSet} for the currently active key.
	 * The caller must ensure that the correct key is set in the state backend.
	 *
	 * <p>The caller must also ensure to properly persist changes to state using
	 * {@link MergingWindowSet#persist()}.
	 */
	/**
	 * 为当前活跃的 key 取回 MergingWindowSet
	 */
	protected MergingWindowSet<W> getMergingWindowSet() throws Exception {
		@SuppressWarnings("unchecked")
		MergingWindowAssigner<? super IN, W> mergingAssigner = (MergingWindowAssigner<? super IN, W>) windowAssigner;
		return new MergingWindowSet<>(mergingAssigner, mergingSetsState);
	}

	/**
	 * Returns {@code true} if the watermark is after the end timestamp plus the allowed lateness
	 * of the given window.
	 */
	/**
	 * 如果当前的 watermark 大于窗口的末端 + 允许的延迟，返回 true
	 */
	protected boolean isWindowLate(W window) {
		return (windowAssigner.isEventTime() && (cleanupTime(window) <= internalTimerService.currentWatermark()));
	}

	/**
	 * Decide if a record is currently late, based on current watermark and allowed lateness.
	 *
	 * @param element The element to check
	 * @return The element for which should be considered when sideoutputs
	 */
	/**
	 * 判断当前 record 是否已经 late，通过当前的 watermark 和允许延迟来判断
	 */
	protected boolean isElementLate(StreamRecord<IN> element){
		return (windowAssigner.isEventTime()) &&
			(element.getTimestamp() + allowedLateness <= internalTimerService.currentWatermark());
	}

	/**
	 * Registers a timer to cleanup the content of the window.
	 * @param window
	 * 					the window whose state to discard
	 */
	/**
	 * 注册一个定时器来清理窗口的内容
	 */
	protected void registerCleanupTimer(W window) {
		long cleanupTime = cleanupTime(window);
		if (cleanupTime == Long.MAX_VALUE) {
			// don't set a GC timer for "end of time"
			return;
		}

		if (windowAssigner.isEventTime()) {
			triggerContext.registerEventTimeTimer(cleanupTime);
		} else {
			triggerContext.registerProcessingTimeTimer(cleanupTime);
		}
	}

	/**
	 * Deletes the cleanup timer set for the contents of the provided window.
	 * @param window the window whose state to discard
	 */
	/**
	 * 删除指定的定时器
	 */
	protected void deleteCleanupTimer(W window) {
		long cleanupTime = cleanupTime(window);
		if (cleanupTime == Long.MAX_VALUE) {
			// no need to clean up because we didn't set one
			return;
		}
		if (windowAssigner.isEventTime()) {
			triggerContext.deleteEventTimeTimer(cleanupTime);
		} else {
			triggerContext.deleteProcessingTimeTimer(cleanupTime);
		}
	}

	/**
	 * Returns the cleanup time for a window, which is
	 * {@code window.maxTimestamp + allowedLateness}. In
	 * case this leads to a value greater than {@link Long#MAX_VALUE}
	 * then a cleanup time of {@link Long#MAX_VALUE} is
	 * returned.
	 *
	 * @param window the window whose cleanup time we are computing.
	 */
	/**
	 * 返回一个窗口的清理时间，等于 window.maxTimestamp + allowedLateness
	 * 如果这个值大于 LONG.MAX_VALUE，函数会返回 LONG.MAX_VALUE
	 */
	private long cleanupTime(W window) {
		// 只有事件时间有延迟
		if (windowAssigner.isEventTime()) {
			long cleanupTime = window.maxTimestamp() + allowedLateness;
			// 当 window.maxTimestamp() + allowedLateness < window.maxTimestamp() 说明溢出了
			return cleanupTime >= window.maxTimestamp() ? cleanupTime : Long.MAX_VALUE;
		} else {
			return window.maxTimestamp();
		}
	}

	/**
	 * Returns {@code true} if the given time is the cleanup time for the given window.
	 */
	/**
	 * 如果给定的时间是给定窗口的最大时间，返回 true
	 */
	protected final boolean isCleanupTime(W window, long time) {
		return time == cleanupTime(window);
	}

	/**
	 * Base class for per-window {@link KeyedStateStore KeyedStateStores}. Used to allow per-window
	 * state access for {@link org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction}.
	 */
	/**
	 * 窗口 key 状态存储的基类。用来允许 ProcessWindowFunction 访问窗口状态
	 */
	public abstract class AbstractPerWindowStateStore extends DefaultKeyedStateStore {

		// we have this in the base class even though it's not used in MergingKeyStore so that
		// we can always set it and ignore what actual implementation we have
		// 计数 MergingKeyStore 没有用到这个变量，我们还是把它放在基类中
		// 这样我们总是能够设置它并且忽略真正实现的时候我们干了什么
		protected W window;

		public AbstractPerWindowStateStore(KeyedStateBackend<?> keyedStateBackend, ExecutionConfig executionConfig) {
			super(keyedStateBackend, executionConfig);
		}
	}

	/**
	 * Special {@link AbstractPerWindowStateStore} that doesn't allow access to per-window state.
	 */
	public class MergingWindowStateStore extends AbstractPerWindowStateStore {

		public MergingWindowStateStore(KeyedStateBackend<?> keyedStateBackend, ExecutionConfig executionConfig) {
			super(keyedStateBackend, executionConfig);
		}

		@Override
		public <T> ValueState<T> getState(ValueStateDescriptor<T> stateProperties) {
			throw new UnsupportedOperationException("Per-window state is not allowed when using merging windows.");
		}

		@Override
		public <T> ListState<T> getListState(ListStateDescriptor<T> stateProperties) {
			throw new UnsupportedOperationException("Per-window state is not allowed when using merging windows.");
		}

		@Override
		public <T> ReducingState<T> getReducingState(ReducingStateDescriptor<T> stateProperties) {
			throw new UnsupportedOperationException("Per-window state is not allowed when using merging windows.");
		}

		@Override
		public <IN, ACC, OUT> AggregatingState<IN, OUT> getAggregatingState(AggregatingStateDescriptor<IN, ACC, OUT> stateProperties) {
			throw new UnsupportedOperationException("Per-window state is not allowed when using merging windows.");
		}

		@Override
		public <T, A> FoldingState<T, A> getFoldingState(FoldingStateDescriptor<T, A> stateProperties) {
			throw new UnsupportedOperationException("Per-window state is not allowed when using merging windows.");
		}

		@Override
		public <UK, UV> MapState<UK, UV> getMapState(MapStateDescriptor<UK, UV> stateProperties) {
			throw new UnsupportedOperationException("Per-window state is not allowed when using merging windows.");
		}
	}

	/**
	 * Regular per-window state store for use with
	 * {@link org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction}.
	 */
	public class PerWindowStateStore extends AbstractPerWindowStateStore {

		public PerWindowStateStore(KeyedStateBackend<?> keyedStateBackend, ExecutionConfig executionConfig) {
			super(keyedStateBackend, executionConfig);
		}

		@Override
		protected  <S extends State> S getPartitionedState(StateDescriptor<S, ?> stateDescriptor) throws Exception {
			return keyedStateBackend.getPartitionedState(
				window,
				windowSerializer,
				stateDescriptor);
		}
	}

	/**
	 * A utility class for handling {@code ProcessWindowFunction} invocations. This can be reused
	 * by setting the {@code key} and {@code window} fields. No internal state must be kept in the
	 * {@code WindowContext}.
	 */
	/**
	 * WindowContext 是用于处理 ProcessWindowFunction 调用的工具
	 * 可以通过设置 key 和 window 来重用，上下文中不能保留任何状态
	 */
	public class WindowContext implements InternalWindowFunction.InternalWindowContext {
		protected W window;  // 保存当前操作的 window，这样作为参数传递的时候就知道 window 是哪个了

		protected AbstractPerWindowStateStore windowState;

		public WindowContext(W window) {
			this.window = window;
			this.windowState = windowAssigner instanceof MergingWindowAssigner ?
				new MergingWindowStateStore(getKeyedStateBackend(), getExecutionConfig()) :
				new PerWindowStateStore(getKeyedStateBackend(), getExecutionConfig());
		}

		@Override
		public String toString() {
			return "WindowContext{Window = " + window.toString() + "}";
		}

		public void clear() throws Exception {
			userFunction.clear(window, this);
		}

		@Override
		public long currentProcessingTime() {
			return internalTimerService.currentProcessingTime();
		}

		@Override
		public long currentWatermark() {
			return internalTimerService.currentWatermark();
		}

		@Override
		public KeyedStateStore windowState() {
			this.windowState.window = this.window;
			return this.windowState;
		}

		@Override
		public KeyedStateStore globalState() {
			return WindowOperator.this.getKeyedStateStore();
		}

		@Override
		public <X> void output(OutputTag<X> outputTag, X value) {
			if (outputTag == null) {
				throw new IllegalArgumentException("OutputTag must not be null.");
			}
			output.collect(outputTag, new StreamRecord<>(value, window.maxTimestamp()));
		}
	}

	/**
	 * {@code Context} is a utility for handling {@code Trigger} invocations. It can be reused
	 * by setting the {@code key} and {@code window} fields. No internal state must be kept in
	 * the {@code Context}
	 */
	/**
	 * Context 是用于处理触发器调用的工具
	 * 可以通过设置 key 和 window 来重用，上下文中不能保留任何状态
	 */
	public class Context implements Trigger.OnMergeContext {
		protected K key;  // 保存当前操作的 key
		protected W window;  // 保存当前操作的 window

		protected Collection<W> mergedWindows;

		public Context(K key, W window) {
			this.key = key;
			this.window = window;
		}

		@Override
		public MetricGroup getMetricGroup() {
			return WindowOperator.this.getMetricGroup();
		}

		// 返回当前的 watermark
		public long getCurrentWatermark() {
			return internalTimerService.currentWatermark();
		}

		@Override
		public <S extends Serializable> ValueState<S> getKeyValueState(String name,
			Class<S> stateType,
			S defaultState) {
			checkNotNull(stateType, "The state type class must not be null");

			TypeInformation<S> typeInfo;
			try {
				typeInfo = TypeExtractor.getForClass(stateType);
			}
			catch (Exception e) {
				throw new RuntimeException("Cannot analyze type '" + stateType.getName() +
					"' from the class alone, due to generic type parameters. " +
					"Please specify the TypeInformation directly.", e);
			}

			return getKeyValueState(name, typeInfo, defaultState);
		}

		@Override
		public <S extends Serializable> ValueState<S> getKeyValueState(String name,
			TypeInformation<S> stateType,
			S defaultState) {

			checkNotNull(name, "The name of the state must not be null");
			checkNotNull(stateType, "The state type information must not be null");

			ValueStateDescriptor<S> stateDesc = new ValueStateDescriptor<>(name, stateType.createSerializer(getExecutionConfig()), defaultState);
			return getPartitionedState(stateDesc);
		}

		@SuppressWarnings("unchecked")
		public <S extends State> S getPartitionedState(StateDescriptor<S, ?> stateDescriptor) {
			try {
				return WindowOperator.this.getPartitionedState(window, windowSerializer, stateDescriptor);
			} catch (Exception e) {
				throw new RuntimeException("Could not retrieve state", e);
			}
		}

		@Override
		public <S extends MergingState<?, ?>> void mergePartitionedState(StateDescriptor<S, ?> stateDescriptor) {
			if (mergedWindows != null && mergedWindows.size() > 0) {
				try {
					S rawState = getKeyedStateBackend().getOrCreateKeyedState(windowSerializer, stateDescriptor);

					if (rawState instanceof InternalMergingState) {
						@SuppressWarnings("unchecked")
						InternalMergingState<K, W, ?, ?, ?> mergingState = (InternalMergingState<K, W, ?, ?, ?>) rawState;
						mergingState.mergeNamespaces(window, mergedWindows);
					}
					else {
						throw new IllegalArgumentException(
								"The given state descriptor does not refer to a mergeable state (MergingState)");
					}
				}
				catch (Exception e) {
					throw new RuntimeException("Error while merging state.", e);
				}
			}
		}

		@Override
		// 获取当前的进程时间
		public long getCurrentProcessingTime() {
			return internalTimerService.currentProcessingTime();
		}

		@Override
		// 注册进程时间定时器
		public void registerProcessingTimeTimer(long time) {
			internalTimerService.registerProcessingTimeTimer(window, time);
		}

		@Override
		// 注册事件时间定时器
		public void registerEventTimeTimer(long time) {
			internalTimerService.registerEventTimeTimer(window, time);
		}

		@Override
		// 删除进程时间定时器
		public void deleteProcessingTimeTimer(long time) {
			internalTimerService.deleteProcessingTimeTimer(window, time);
		}

		@Override
		// 删除事件时间定时器
		public void deleteEventTimeTimer(long time) {
			internalTimerService.deleteEventTimeTimer(window, time);
		}

		public TriggerResult onElement(StreamRecord<IN> element) throws Exception {
			return trigger.onElement(element.getValue(), element.getTimestamp(), window, this);
		}

		public TriggerResult onProcessingTime(long time) throws Exception {
			return trigger.onProcessingTime(time, window, this);
		}

		public TriggerResult onEventTime(long time) throws Exception {
			return trigger.onEventTime(time, window, this);
		}

		public void onMerge(Collection<W> mergedWindows) throws Exception {
			this.mergedWindows = mergedWindows;
			trigger.onMerge(window, this);
		}

		public void clear() throws Exception {
			trigger.clear(window, this);
		}

		@Override
		public String toString() {
			return "Context{" +
				"key=" + key +
				", window=" + window +
				'}';
		}
	}

	/**
	 * Internal class for keeping track of in-flight timers.
	 */
	// 用于跟踪 in-flight 定时器的内部类
	protected static class Timer<K, W extends Window> implements Comparable<Timer<K, W>> {
		protected long timestamp;
		protected K key;
		protected W window;

		public Timer(long timestamp, K key, W window) {
			this.timestamp = timestamp;
			this.key = key;
			this.window = window;
		}

		@Override
		public int compareTo(Timer<K, W> o) {
			return Long.compare(this.timestamp, o.timestamp);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()){
				return false;
			}

			Timer<?, ?> timer = (Timer<?, ?>) o;

			return timestamp == timer.timestamp
				&& key.equals(timer.key)
				&& window.equals(timer.window);

		}

		@Override
		public int hashCode() {
			int result = (int) (timestamp ^ (timestamp >>> 32));
			result = 31 * result + key.hashCode();
			result = 31 * result + window.hashCode();
			return result;
		}

		@Override
		public String toString() {
			return "Timer{" +
				"timestamp=" + timestamp +
				", key=" + key +
				", window=" + window +
				'}';
		}
	}

	// ------------------------------------------------------------------------
	// Getters for testing
	// ------------------------------------------------------------------------

	@VisibleForTesting
	public Trigger<? super IN, ? super W> getTrigger() {
		return trigger;
	}

	@VisibleForTesting
	public KeySelector<IN, K> getKeySelector() {
		return keySelector;
	}

	@VisibleForTesting
	public WindowAssigner<? super IN, W> getWindowAssigner() {
		return windowAssigner;
	}

	@VisibleForTesting
	public StateDescriptor<? extends AppendingState<IN, ACC>, ?> getStateDescriptor() {
		return windowStateDescriptor;
	}
}
