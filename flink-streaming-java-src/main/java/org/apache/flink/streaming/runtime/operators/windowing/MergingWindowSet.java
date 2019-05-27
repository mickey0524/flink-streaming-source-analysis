/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.operators.windowing;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.windowing.assigners.MergingWindowAssigner;
import org.apache.flink.streaming.api.windowing.windows.Window;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility for keeping track of merging {@link Window Windows} when using a
 * {@link MergingWindowAssigner} in a {@link WindowOperator}.
 *
 * 当在 WindowOperator 中使用 MergingWindowAssigner 时候
 * 跟踪合并窗口的工具
 *
 * <p>When merging windows, we keep one of the original windows as the state window, i.e. the
 * window that is used as namespace to store the window elements. Elements from the state
 * windows of merged windows must be merged into this one state window. We keep
 * a mapping from in-flight window to state window that can be queried using
 * {@link #getStateWindow(Window)}.
 *
 * 当合并窗口的时候，我们将原始窗口中的一个设置为状态窗口，这个窗口被用作命名空间来存储窗口元素
 * 合并窗口的状态窗口的元素必须被合并到这个指定的状态窗口
 * 我们保留一个从飞行窗口到状态窗口的映射，可以使用 getstatewindow(window) 查询该映射
 *
 * <p>A new window can be added to the set of in-flight windows using
 * {@link #addWindow(Window, MergeFunction)}. This might merge other windows and the caller
 * must react accordingly in the {@link MergeFunction#merge(Object, Collection, Object, Collection)}
 * and adjust the outside view of windows and state.
 *
 * 一个新的窗口可以通过 addWindow(Window, MergeFunction) 来加入飞行窗口的集合
 * 这可能会合并其他窗口，调用方必须相应地在合并函数中作出反应并调整窗口和状态的外部视图
 *
 * <p>Windows can be removed from the set of windows using {@link #retireWindow(Window)}.
 *
 * 窗口可以通过 retireWindow(Window) 方法被移除窗口集合
 * 
 * @param <W> The type of {@code Window} that this set is keeping track of.
 */
public class MergingWindowSet<W extends Window> {

	private static final Logger LOG = LoggerFactory.getLogger(MergingWindowSet.class);

	/**
	 * Mapping from window to the window that keeps the window state. When
	 * we are incrementally merging windows starting from some window we keep that starting
	 * window as the state window to prevent costly state juggling.
	 */
	/**
	 * 窗口到窗口的映射来保持窗口状态。当我们从某个窗口开始逐步合并窗口时
	 * 我们将该开始窗口保留为状态窗口，以防止昂贵的状态变化
	 */
	private final Map<W, W> mapping;

	/**
	 * Mapping when we created the {@code MergingWindowSet}. We use this to decide whether
	 * we need to persist any changes to state.
	 */
	/**
	 * 我们创建 MergingWindowSet 时候的映射，我们使用这个来决定我们是否需要持久化操作到 state 中
	 */
	private final Map<W, W> initialMapping;

	private final ListState<Tuple2<W, W>> state;

	/**
	 * Our window assigner.
	 */
	// 合并窗口分配器
	private final MergingWindowAssigner<?, W> windowAssigner;

	/**
	 * Restores a {@link MergingWindowSet} from the given state.
	 */
	/**
	 * 从给定的 state 中恢复一个 MergingWindowSet
	 */
	public MergingWindowSet(MergingWindowAssigner<?, W> windowAssigner, ListState<Tuple2<W, W>> state) throws Exception {
		this.windowAssigner = windowAssigner;
		mapping = new HashMap<>();

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

	/**
	 * Persist the updated mapping to the given state if the mapping changed since
	 * initialization.
	 */
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

	/**
	 * Returns the state window for the given in-flight {@code Window}. The state window is the
	 * {@code Window} in which we keep the actual state of a given in-flight window. Windows
	 * might expand but we keep to original state window for keeping the elements of the window
	 * to avoid costly state juggling.
	 *
	 * @param window The window for which to get the state window.
	 */
	/**
	 * 返回给定窗口的状态窗口。我们用状态窗口来保存飞行窗口的真正状态
	 * 窗口可能会扩展，但是我们保留了最初的映射，这样我们可以避免昂贵的窗口变化
	 */
	public W getStateWindow(W window) {
		return mapping.get(window);
	}

	/**
	 * Removes the given window from the set of in-flight windows.
	 *
	 * @param window The {@code Window} to remove.
	 */
	/**
	 * 删除给定窗口
	 */
	public void retireWindow(W window) {
		W removed = this.mapping.remove(window);
		if (removed == null) {
			throw new IllegalStateException("Window " + window + " is not in in-flight window set.");
		}
	}

	/**
	 * Adds a new {@code Window} to the set of in-flight windows. It might happen that this
	 * triggers merging of previously in-flight windows. In that case, the provided
	 * {@link MergeFunction} is called.
	 *
	 * 添加一个新的窗口到飞行窗口的集合，这可能会触发先前飞行窗口的合并
	 * 这种情况下，给出的 MergeFunction 被调用
	 *
	 * <p>This returns the window that is the representative of the added window after adding.
	 * This can either be the new window itself, if no merge occurred, or the newly merged
	 * window. Adding an element to a window or calling trigger functions should only
	 * happen on the returned representative. This way, we never have to deal with a new window
	 * that is immediately swallowed up by another window.
	 *
	 * 这个操作将返回添加操作后代表添加窗口的窗口，如果没有发生合并，这将是新窗口本身
	 * 否则将是新合并的窗口。应该在返回的代表上添加元素或者调用触发器函数
	 * 这样，我们不用处理一个新的窗口，因为它马上就被其他窗口吞没了
	 *
	 * <p>If the new window is merged, the {@code MergeFunction} callback arguments also don't
	 * contain the new window as part of the list of merged windows.
	 *
	 * 如果新窗口被合并，MergeFunction 回调参数也不包含新窗口作为合并窗口列表的一部分
	 *
	 * @param newWindow The new {@code Window} to add.
	 * @param mergeFunction The callback to be invoked in case a merge occurs.
	 *
	 * @return The {@code Window} that new new {@code Window} ended up in. This can also be the
	 *          the new {@code Window} itself in case no merge occurred.
	 * @throws Exception
	 */
	public W addWindow(W newWindow, MergeFunction<W> mergeFunction) throws Exception {

		List<W> windows = new ArrayList<>();

		windows.addAll(this.mapping.keySet());
		windows.add(newWindow);

		// k 是合并后的窗口，v 是合并后的窗口包含的原始窗口集合
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

			// if our new window is in the merged windows make the merge result the
			// result window
			// 如果新窗口位于合并窗口中，则将合并结果设为结果窗口
			// 这里也感觉可以优化，因为 TimeWindow 的 mergeWindows 方法实现
			// 保证一个 window 仅仅存在于一个 mergedWindows 中
			if (mergedWindows.remove(newWindow)) {
				mergedNewWindow = true;
				resultWindow = mergeResult;
			}

			// pick any of the merged windows and choose that window's state window
			// as the state window for the merge result
			// 选择任意一个窗口，并选择该窗口的状态窗口作为合并结果的状态窗口
			W mergedStateWindow = this.mapping.get(mergedWindows.iterator().next());

			// figure out the state windows that we are merging
			// 把当前 mapping 中的 key 存在于 mergedWindows 中的 (k, v) 全部删除
			List<W> mergedStateWindows = new ArrayList<>();
			for (W mergedWindow: mergedWindows) {
				W res = this.mapping.remove(mergedWindow);
				if (res != null) {
					mergedStateWindows.add(res);  // 合并集合中所有 k 的 v 组成的集合
				}
			}

			this.mapping.put(mergeResult, mergedStateWindow);

			// don't put the target state window into the merged windows
			// 需要把我们用来做合并结果状态的窗口删除
			mergedStateWindows.remove(mergedStateWindow);

			// don't merge the new window itself, it never had any state associated with it
			// i.e. if we are only merging one pre-existing window into itself
			// without extending the pre-existing window
			// 不要合并新的窗口自己，它没有任何与其相关的状态
			if (!(mergedWindows.contains(mergeResult) && mergedWindows.size() == 1)) {
				mergeFunction.merge(mergeResult,
						mergedWindows,
						this.mapping.get(mergeResult),
						mergedStateWindows);
			}
		}

		// the new window created a new, self-contained window without merging
		// 窗口没有被 merge，成为了一个新的窗口
		if (mergeResults.isEmpty() || (resultWindow.equals(newWindow) && !mergedNewWindow)) {
			this.mapping.put(resultWindow, resultWindow);
		}

		return resultWindow;
	}

	/**
	 * Callback for {@link #addWindow(Window, MergeFunction)}.
	 * @param <W>
	 */
	public interface MergeFunction<W> {

		/**
		 * This gets called when a merge occurs.
		 *
		 * @param mergeResult The newly resulting merged {@code Window}.
		 * @param mergedWindows The merged {@code Window Windows}.
		 * @param stateWindowResult The state window of the merge result.
		 * @param mergedStateWindows The merged state windows.
		 * @throws Exception
		 */
		void merge(W mergeResult, Collection<W> mergedWindows, W stateWindowResult, Collection<W> mergedStateWindows) throws Exception;
	}

	@Override
	public String toString() {
		return "MergingWindowSet{" +
				"windows=" + mapping +
				'}';
	}
}
