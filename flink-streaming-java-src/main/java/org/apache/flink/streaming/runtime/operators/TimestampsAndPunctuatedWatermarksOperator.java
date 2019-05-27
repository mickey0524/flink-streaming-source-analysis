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

import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * A stream operator that extracts timestamps from stream elements and
 * generates watermarks based on punctuation elements.
 *
 * @param <T> The type of the input elements
 */
/**
 * 流运算符，从流元素中提取时间戳，并根据标点元素生成水印
 */
public class TimestampsAndPunctuatedWatermarksOperator<T>
		extends AbstractUdfStreamOperator<T, AssignerWithPunctuatedWatermarks<T>>
		implements OneInputStreamOperator<T, T> {

	private static final long serialVersionUID = 1L;

	private long currentWatermark = Long.MIN_VALUE;

	public TimestampsAndPunctuatedWatermarksOperator(AssignerWithPunctuatedWatermarks<T> assigner) {
		super(assigner);
		this.chainingStrategy = ChainingStrategy.ALWAYS;
	}

	@Override
	public void processElement(StreamRecord<T> element) throws Exception {
		final T value = element.getValue();
		final long newTimestamp = userFunction.extractTimestamp(value,
				element.hasTimestamp() ? element.getTimestamp() : Long.MIN_VALUE);

		output.collect(element.replace(element.getValue(), newTimestamp));

		final Watermark nextWatermark = userFunction.checkAndGetNextWatermark(value, newTimestamp);
		if (nextWatermark != null && nextWatermark.getTimestamp() > currentWatermark) {
			currentWatermark = nextWatermark.getTimestamp();
			output.emitWatermark(nextWatermark);
		}
	}

	/**
	 * Override the base implementation to completely ignore watermarks propagated from
	 * upstream (we rely only on the {@link AssignerWithPunctuatedWatermarks} to emit
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
}
