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

package org.apache.flink.streaming.api.datastream;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.StreamTransformation;

import java.util.Arrays;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * A {@code BroadcastStream} is a stream with {@link org.apache.flink.api.common.state.BroadcastState broadcast state(s)}.
 * This can be created by any stream using the {@link DataStream#broadcast(MapStateDescriptor[])} method and
 * implicitly creates states where the user can store elements of the created {@code BroadcastStream}.
 * (see {@link BroadcastConnectedStream}).
 *
 * 一个 BroadcastStream 是一个有着 BroadcastState 的流
 * BroadcastStream 能够被任何流执行 broadcast(MapStateDescriptor[]) 生成
 * BroadcastStream 隐式的创建了 states，用户可以在 states 里面存储元素
 *
 * <p>Note that no further operation can be applied to these streams. The only available option is to connect them
 * with a keyed or non-keyed stream, using the {@link KeyedStream#connect(BroadcastStream)} and the
 * {@link DataStream#connect(BroadcastStream)} respectively. Applying these methods will result it a
 * {@link BroadcastConnectedStream} for further processing.
 *
 * 需要注意的是，BroadcastStream 没有其他的操作
 * 唯一的操作是 DataStream 或 KeyedStream 能够 connect BroadcastStream 生成 BroadcastConnectedStream
 *
 * @param <T> The type of input/output elements.
 */
@PublicEvolving
public class BroadcastStream<T> {

	private final StreamExecutionEnvironment environment;

	private final DataStream<T> inputStream;

	/**
	 * The {@link org.apache.flink.api.common.state.StateDescriptor state descriptors} of the
	 * registered {@link org.apache.flink.api.common.state.BroadcastState broadcast states}. These
	 * states have {@code key-value} format.
	 */
	/**
	 * 广播状态的描述符 list
	 * 这些状态是以 key-value 格式存储的
	 */
	private final List<MapStateDescriptor<?, ?>> broadcastStateDescriptors;

	protected BroadcastStream(
			final StreamExecutionEnvironment env,
			final DataStream<T> input,
			final MapStateDescriptor<?, ?>... broadcastStateDescriptors) {

		this.environment = requireNonNull(env);
		this.inputStream = requireNonNull(input);
		this.broadcastStateDescriptors = Arrays.asList(requireNonNull(broadcastStateDescriptors));
	}

	public TypeInformation<T> getType() {
		return inputStream.getType();
	}

	public <F> F clean(F f) {
		return environment.clean(f);
	}

	public StreamTransformation<T> getTransformation() {
		return inputStream.getTransformation();
	}

	public List<MapStateDescriptor<?, ?>> getBroadcastStateDescriptor() {
		return broadcastStateDescriptors;
	}

	public StreamExecutionEnvironment getEnvironment() {
		return environment;
	}
}
