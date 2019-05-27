/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.transformations;

import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.util.OutputTag;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import java.util.Collection;
import java.util.List;

import static java.util.Objects.requireNonNull;


/**
 * This transformation represents a selection of a side output of an upstream operation with a
 * given {@link OutputTag}.
 *
 * <p>This does not create a physical operation, it only affects how upstream operations are
 * connected to downstream operations.
 *
 * @param <T> The type of the elements that result from this {@code SideOutputTransformation}
 */
/**
 * 上游的偏侧输出
 *
 * [https://blog.51cto.com/1196740/2357420](侧边输出用法)
 *
 * 只有 ProcessFunction 中的 ctx.output 能向偏侧写入数据
 * 然后 SingleOutputStreamOperator.java 中使用 getSideOutput 获取偏侧输出
 * 定义 outputTag 的时候需要创建一个匿名内部类
 * OutputTag<String> outputTag = new OutputTag<String>("side-output") {};
 */
public class SideOutputTransformation<T> extends StreamTransformation<T> {
	private final StreamTransformation<?> input;

	private final OutputTag<T> tag;

	public SideOutputTransformation(StreamTransformation<?> input, final OutputTag<T> tag) {
		super("SideOutput", tag.getTypeInfo(), requireNonNull(input).getParallelism());
		this.input = input;
		this.tag = requireNonNull(tag);
	}

	/**
	 * Returns the input {@code StreamTransformation}.
	 */
	public StreamTransformation<?> getInput() {
		return input;
	}

	public OutputTag<T> getOutputTag() {
		return tag;
	}

	@Override
	public Collection<StreamTransformation<?>> getTransitivePredecessors() {
		List<StreamTransformation<?>> result = Lists.newArrayList();
		result.add(this);
		result.addAll(input.getTransitivePredecessors());
		return result;
	}

	@Override
	public final void setChainingStrategy(ChainingStrategy strategy) {
		throw new UnsupportedOperationException("Cannot set chaining strategy on SideOutput Transformation.");
	}
}
