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

package org.apache.flink.streaming.api.operators.co;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction.Context;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link TwoInputStreamOperator} for executing {@link BroadcastProcessFunction BroadcastProcessFunctions}.
 *
 * @param <IN1> The input type of the keyed (non-broadcast) side.
 * @param <IN2> The input type of the broadcast side.
 * @param <OUT> The output type of the operator.
 */
/**
 * 一个 TwoInputStreamOperator 执行 BroadcastProcessFunctions
 */
@Internal
public class CoBroadcastWithNonKeyedOperator<IN1, IN2, OUT>
		extends AbstractUdfStreamOperator<OUT, BroadcastProcessFunction<IN1, IN2, OUT>>
		implements TwoInputStreamOperator<IN1, IN2, OUT> {

	private static final long serialVersionUID = -1869740381935471752L;

	/** We listen to this ourselves because we don't have an {@link InternalTimerService}. */
	// 非 keyed 的 Operator 只能自己记录 Watermark
	private long currentWatermark = Long.MIN_VALUE;
	
	// BroadcastStream 的 stateDescriptor
	private final List<MapStateDescriptor<?, ?>> broadcastStateDescriptors;

	// 保证 emit 的元素的 ts 是相同的
	private transient TimestampedCollector<OUT> collector;

	private transient Map<MapStateDescriptor<?, ?>, BroadcastState<?, ?>> broadcastStates;

	private transient ReadWriteContextImpl rwContext;  // 给广播流使用的

	private transient ReadOnlyContextImpl rContext;  // 给非广播流使用的

	public CoBroadcastWithNonKeyedOperator(
			final BroadcastProcessFunction<IN1, IN2, OUT> function,
			final List<MapStateDescriptor<?, ?>> broadcastStateDescriptors) {
		super(function);
		this.broadcastStateDescriptors = Preconditions.checkNotNull(broadcastStateDescriptors);
	}

	@Override
	public void open() throws Exception {
		super.open();

		collector = new TimestampedCollector<>(output);

		this.broadcastStates = new HashMap<>(broadcastStateDescriptors.size());
		for (MapStateDescriptor<?, ?> descriptor: broadcastStateDescriptors) {
			// 非 keyed，状态存储在 OperatorStateBackend 中
			broadcastStates.put(descriptor, getOperatorStateBackend().getBroadcastState(descriptor));
		}

		rwContext = new ReadWriteContextImpl(getExecutionConfig(), userFunction, broadcastStates, getProcessingTimeService());
		rContext = new ReadOnlyContextImpl(getExecutionConfig(), userFunction, broadcastStates, getProcessingTimeService());
	}

	// 数据流侧
	@Override
	public void processElement1(StreamRecord<IN1> element) throws Exception {
		collector.setTimestamp(element);
		rContext.setElement(element);
		userFunction.processElement(element.getValue(), rContext, collector);
		rContext.setElement(null);
	}

	// 广播流侧
	@Override
	public void processElement2(StreamRecord<IN2> element) throws Exception {
		collector.setTimestamp(element);
		rwContext.setElement(element);
		userFunction.processBroadcastElement(element.getValue(), rwContext, collector);
		rwContext.setElement(null);
	}

	// 更新 currentWatermark
	@Override
	public void processWatermark(Watermark mark) throws Exception {
		super.processWatermark(mark);
		currentWatermark = mark.getTimestamp();
	}

	// 广播流侧的上下文
	private class ReadWriteContextImpl extends Context {

		private final ExecutionConfig config;

		private final Map<MapStateDescriptor<?, ?>, BroadcastState<?, ?>> states;

		private final ProcessingTimeService timerService;

		private StreamRecord<IN2> element;

		ReadWriteContextImpl(
				final ExecutionConfig executionConfig,
				final BroadcastProcessFunction<IN1, IN2, OUT> function,
				final Map<MapStateDescriptor<?, ?>, BroadcastState<?, ?>> broadcastStates,
				final ProcessingTimeService timerService) {

			function.super();
			this.config = Preconditions.checkNotNull(executionConfig);
			this.states = Preconditions.checkNotNull(broadcastStates);
			this.timerService = Preconditions.checkNotNull(timerService);
		}

		void setElement(StreamRecord<IN2> e) {
			this.element = e;
		}

		@Override
		public Long timestamp() {
			checkState(element != null);
			return element.getTimestamp();
		}

		// 从 map 中获取对应的 state
		@Override
		public <K, V> BroadcastState<K, V> getBroadcastState(MapStateDescriptor<K, V> stateDescriptor) {
			Preconditions.checkNotNull(stateDescriptor);

			stateDescriptor.initializeSerializerUnlessSet(config);
			BroadcastState<K, V> state = (BroadcastState<K, V>) states.get(stateDescriptor);
			if (state == null) {
				throw new IllegalArgumentException("The requested state does not exist. " +
						"Check for typos in your state descriptor, or specify the state descriptor " +
						"in the datastream.broadcast(...) call if you forgot to register it.");
			}
			return state;
		}

		// 侧边输出
		@Override
		public <X> void output(OutputTag<X> outputTag, X value) {
			checkArgument(outputTag != null, "OutputTag must not be null.");
			output.collect(outputTag, new StreamRecord<>(value, element.getTimestamp()));
		}

		// 返回当前进程时间
		@Override
		public long currentProcessingTime() {
			return timerService.getCurrentProcessingTime();
		}

		// 返回当前 watermark
		@Override
		public long currentWatermark() {
			return currentWatermark;
		}
	}

	// 广播流侧的上下文，和 ReadWriteContextImpl 相同
	private class ReadOnlyContextImpl extends BroadcastProcessFunction<IN1, IN2, OUT>.ReadOnlyContext {

		private final ExecutionConfig config;

		private final Map<MapStateDescriptor<?, ?>, BroadcastState<?, ?>> states;

		private final ProcessingTimeService timerService;

		private StreamRecord<IN1> element;

		ReadOnlyContextImpl(
				final ExecutionConfig executionConfig,
				final BroadcastProcessFunction<IN1, IN2, OUT> function,
				final Map<MapStateDescriptor<?, ?>, BroadcastState<?, ?>> broadcastStates,
				final ProcessingTimeService timerService) {

			function.super();
			this.config = Preconditions.checkNotNull(executionConfig);
			this.states = Preconditions.checkNotNull(broadcastStates);
			this.timerService = Preconditions.checkNotNull(timerService);
		}

		void setElement(StreamRecord<IN1> e) {
			this.element = e;
		}

		@Override
		public Long timestamp() {
			checkState(element != null);
			return element.hasTimestamp() ? element.getTimestamp() : null;
		}

		@Override
		public <X> void output(OutputTag<X> outputTag, X value) {
			checkArgument(outputTag != null, "OutputTag must not be null.");
			output.collect(outputTag, new StreamRecord<>(value, element.getTimestamp()));
		}

		@Override
		public long currentProcessingTime() {
			return timerService.getCurrentProcessingTime();
		}

		@Override
		public long currentWatermark() {
			return currentWatermark;
		}

		@Override
		public <K, V> ReadOnlyBroadcastState<K, V> getBroadcastState(MapStateDescriptor<K, V> stateDescriptor) {
			Preconditions.checkNotNull(stateDescriptor);

			stateDescriptor.initializeSerializerUnlessSet(config);
			// 得到的是 ReadOnlyBroadcastState
			ReadOnlyBroadcastState<K, V> state = (ReadOnlyBroadcastState<K, V>) states.get(stateDescriptor);
			if (state == null) {
				throw new IllegalArgumentException("The requested state does not exist. " +
						"Check for typos in your state descriptor, or specify the state descriptor " +
						"in the datastream.broadcast(...) call if you forgot to register it.");
			}
			return state;
		}
	}
}
