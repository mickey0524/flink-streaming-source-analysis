/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.collector.selector;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.metrics.WatermarkGauge;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.OperatorChain;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.XORShiftRandom;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * Wrapping {@link Output} that forwards to other {@link Output Outputs } based on a list of
 * {@link OutputSelector OutputSelectors}.
 */
/**
 * 包装根据输出选择器列表转发到其他输出的输出
 */
public class DirectedOutput<OUT> implements OperatorChain.WatermarkGaugeExposingOutput<StreamRecord<OUT>> {

	protected final OutputSelector<OUT>[] outputSelectors;  // 输出选择器，split 方法的参数

	protected final Output<StreamRecord<OUT>>[] selectAllOutputs;

	protected final HashMap<String, Output<StreamRecord<OUT>>[]> outputMap;

	protected final Output<StreamRecord<OUT>>[] allOutputs;  // 所有的输出

	private final Random random = new XORShiftRandom();

	protected final WatermarkGauge watermarkGauge = new WatermarkGauge();

	@SuppressWarnings({"unchecked", "rawtypes"})
	public DirectedOutput(
			List<OutputSelector<OUT>> outputSelectors,
			List<? extends Tuple2<? extends Output<StreamRecord<OUT>>, StreamEdge>> outputs) {
		this.outputSelectors = outputSelectors.toArray(new OutputSelector[outputSelectors.size()]);

		this.allOutputs = new Output[outputs.size()];
		for (int i = 0; i < outputs.size(); i++) {
			allOutputs[i] = outputs.get(i).f0;
		}

		// StreamEdge 上没有 selectedName，将 output 写入 selectAllOutputs
		HashSet<Output<StreamRecord<OUT>>> selectAllOutputs = new HashSet<Output<StreamRecord<OUT>>>();
		// StreamEdge 有 selectedName，根据 selectedName 进行区分
		HashMap<String, ArrayList<Output<StreamRecord<OUT>>>> outputMap = new HashMap<String, ArrayList<Output<StreamRecord<OUT>>>>();

		for (Tuple2<? extends Output<StreamRecord<OUT>>, StreamEdge> outputPair : outputs) {
			final Output<StreamRecord<OUT>> output = outputPair.f0;  // collector output
			final StreamEdge edge = outputPair.f1;  // 输出边

			List<String> selectedNames = edge.getSelectedNames();  // select 方法的参数

			if (selectedNames.isEmpty()) {
				selectAllOutputs.add(output);
			}
			else {
				for (String selectedName : selectedNames) {
					if (!outputMap.containsKey(selectedName)) {
						outputMap.put(selectedName, new ArrayList<Output<StreamRecord<OUT>>>());
						outputMap.get(selectedName).add(output);
					}
					else {
						if (!outputMap.get(selectedName).contains(output)) {
							outputMap.get(selectedName).add(output);
						}
					}
				}
			}
		}

		this.selectAllOutputs = selectAllOutputs.toArray(new Output[selectAllOutputs.size()]);

		this.outputMap = new HashMap<>();
		for (Map.Entry<String, ArrayList<Output<StreamRecord<OUT>>>> entry : outputMap.entrySet()) {
			Output<StreamRecord<OUT>>[] arr = entry.getValue().toArray(new Output[entry.getValue().size()]);
			this.outputMap.put(entry.getKey(), arr);
		}
	}

	@Override
	public void emitWatermark(Watermark mark) {
		watermarkGauge.setCurrentWatermark(mark.getTimestamp());
		for (Output<StreamRecord<OUT>> out : allOutputs) {
			out.emitWatermark(mark);
		}
	}

	@Override
	public void emitLatencyMarker(LatencyMarker latencyMarker) {
		// randomly select an output
		// 随机选择一个输出
		allOutputs[random.nextInt(allOutputs.length)].emitLatencyMarker(latencyMarker);
	}

	protected Set<Output<StreamRecord<OUT>>> selectOutputs(StreamRecord<OUT> record)  {
		// 全输出的 output 没有限制，直接 addAll
		Set<Output<StreamRecord<OUT>>> selectedOutputs = new HashSet<>(selectAllOutputs.length);
		Collections.addAll(selectedOutputs, selectAllOutputs);

		for (OutputSelector<OUT> outputSelector : outputSelectors) {
			// 根据 split 中的 outputSelector 求出 record 的 outputNames
			Iterable<String> outputNames = outputSelector.select(record.getValue());

			// 然后根据 outputNames 与 select 中的进行比对，匹配上了才输出
			for (String outputName : outputNames) {
				Output<StreamRecord<OUT>>[] outputList = outputMap.get(outputName);
				if (outputList != null) {
					Collections.addAll(selectedOutputs, outputList);
				}
			}
		}

		return selectedOutputs;
	}

	@Override
	public void collect(StreamRecord<OUT> record) {
		Set<Output<StreamRecord<OUT>>> selectedOutputs = selectOutputs(record);

		for (Output<StreamRecord<OUT>> out : selectedOutputs) {
			out.collect(record);
		}
	}

	@Override
	/**
	 * split，select 和 side output 是互斥的
	 */
	public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {
		throw new UnsupportedOperationException("Cannot use split/select with side outputs.");
	}

	@Override
	public void close() {
		for (Output<StreamRecord<OUT>> out : allOutputs) {
			out.close();
		}
	}

	@Override
	public Gauge<Long> getWatermarkGauge() {
		return watermarkGauge;
	}
}
