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

package org.apache.flink.streaming.runtime.operators;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;

/**
 * A stream operator that extracts timestamps from stream elements and
 * generates periodic watermarks.
 *
 * @param <T> The type of the input elements
 */
/**
 * 流运算符，从流元素中提取时间戳并生成定期水印
 */
public class TimestampsAndPeriodicWatermarksOperator<T>
		extends AbstractUdfStreamOperator<T, AssignerWithPeriodicWatermarks<T>>
		implements OneInputStreamOperator<T, T>, ProcessingTimeCallback {

	private static final long serialVersionUID = 1L;

	private transient long watermarkInterval;

	private transient long currentWatermark;

	public TimestampsAndPeriodicWatermarksOperator(AssignerWithPeriodicWatermarks<T> assigner) {
		super(assigner);
		this.chainingStrategy = ChainingStrategy.ALWAYS;
	}

	@Override
	public void open() throws Exception {
		super.open();

		currentWatermark = Long.MIN_VALUE;  // 初始化当前水印
		// 获取定时生成水印的间隔
		watermarkInterval = getExecutionConfig().getAutoWatermarkInterval();

		if (watermarkInterval > 0) {
			long now = getProcessingTimeService().getCurrentProcessingTime();
			getProcessingTimeService().registerTimer(now + watermarkInterval, this);
		}
	}

	@Override
	public void processElement(StreamRecord<T> element) throws Exception {
		final long newTimestamp = userFunction.extractTimestamp(element.getValue(),
				element.hasTimestamp() ? element.getTimestamp() : Long.MIN_VALUE);

		output.collect(element.replace(element.getValue(), newTimestamp));
	}

	@Override
	public void onProcessingTime(long timestamp) throws Exception {
		// register next timer
		Watermark newWatermark = userFunction.getCurrentWatermark();
		if (newWatermark != null && newWatermark.getTimestamp() > currentWatermark) {
			currentWatermark = newWatermark.getTimestamp();
			// emit watermark
			// 发出水印
			output.emitWatermark(newWatermark);
		}

		// 注册下一个定时器，emit watermark
		long now = getProcessingTimeService().getCurrentProcessingTime();
		getProcessingTimeService().registerTimer(now + watermarkInterval, this);
	}

	/**
	 * Override the base implementation to completely ignore watermarks propagated from
	 * upstream (we rely only on the {@link AssignerWithPeriodicWatermarks} to emit
	 * watermarks from here).
	 */
	/**
	 * 覆盖基础实现以完全忽略从上游传播的水印（我们仅依靠AssignerWithPeriodicWatermarks从此处发出水印）
	 */
	@Override
	public void processWatermark(Watermark mark) throws Exception {
		// if we receive a Long.MAX_VALUE watermark we forward it since it is used
		// to signal the end of input and to not block watermark progress downstream
		// 如果我们收到Long.MAX_VALUE水印，我们转发它，因为它用于表示输入的结束并且不阻止下游的水印进度
		if (mark.getTimestamp() == Long.MAX_VALUE && currentWatermark != Long.MAX_VALUE) {
			currentWatermark = Long.MAX_VALUE;
			output.emitWatermark(mark);
		}
	}

	@Override
	public void close() throws Exception {
		super.close();

		// emit a final watermark
		Watermark newWatermark = userFunction.getCurrentWatermark();
		if (newWatermark != null && newWatermark.getTimestamp() > currentWatermark) {
			currentWatermark = newWatermark.getTimestamp();
			// emit watermark
			output.emitWatermark(newWatermark);
		}
	}
}
