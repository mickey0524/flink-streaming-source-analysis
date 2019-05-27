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

package org.apache.flink.streaming.api;

import org.apache.flink.annotation.Public;

/**
 * The checkpointing mode defines what consistency guarantees the system gives in the presence of
 * failures.
 *
 * <p>When checkpointing is activated, the data streams are replayed such that lost parts of the
 * processing are repeated. For stateful operations and functions, the checkpointing mode defines
 * whether the system draws checkpoints such that a recovery behaves as if the operators/functions
 * see each record "exactly once" ({@link #EXACTLY_ONCE}), or whether the checkpoints are drawn
 * in a simpler fashion that typically encounters some duplicates upon recovery
 * ({@link #AT_LEAST_ONCE})</p>
 */
/**
 * CheckpointingMode 定义了系统在出错时候的一致性保证
 * 当检查点是活跃的，数据流会被 replayed 以便处理丢失的部分
 * 对于有状态的操作和函数，CheckpointingMode 定义了系统是否绘制
 * 检查点使得一个恢复行为就像操作符/函数仅仅看到每条记录一次一样
 * 或者是否检查点绘制一个简单的，这样通常在恢复的时候会出现一些重复
 */
@Public
public enum CheckpointingMode {

	/**
	 * Sets the checkpointing mode to "exactly once". This mode means that the system will
	 * checkpoint the operator and user function state in such a way that, upon recovery,
	 * every record will be reflected exactly once in the operator state.
	 *
	 * <p>For example, if a user function counts the number of elements in a stream,
	 * this number will consistently be equal to the number of actual elements in the stream,
	 * regardless of failures and recovery.</p>
	 *
	 * <p>Note that this does not mean that each record flows through the streaming data flow
	 * only once. It means that upon recovery, the state of operators/functions is restored such
	 * that the resumed data streams pick up exactly at after the last modification to the state.</p>
	 *
	 * 需要注意的是，exactly_once 并不意味着每一条 record 仅仅在数据流中流动一次
	 * exactly_once 意味着在 flink 恢复的时候，操作符／函数的状态是保存着的，因此
	 * 恢复的数据流准确的选择上一次修改状态的位置
	 *
	 * <p>Note that this mode does not guarantee exactly-once behavior in the interaction with
	 * external systems (only state in Flink's operators and user functions). The reason for that
	 * is that a certain level of "collaboration" is required between two systems to achieve
	 * exactly-once guarantees. However, for certain systems, connectors can be written that facilitate
	 * this collaboration.</p>
	 *
	 * 需要注意的是，exactly_once 并不保证在与外部系统交互的时候的 exactly_once 行为（仅仅保证了 flink 内部操作符和
	 * 用户定义函数的状态）。其原因是两个系统之间需要一定程度的“协作”才能实现一次性保证
	 * 但是，对于某些系统，可以编写便于此协作的连接器
	 *
	 * <p>This mode sustains high throughput. Depending on the data flow graph and operations,
	 * this mode may increase the record latency, because operators need to align their input
	 * streams, in order to create a consistent snapshot point. The latency increase for simple
	 * dataflows (no repartitioning) is negligible. For simple dataflows with repartitioning, the average
	 * latency remains small, but the slowest records typically have an increased latency.</p>
	 *
	 * 这个模式维持着高吞吐。由于依赖数据流图和操作，这个模式有可能会增加 record 的延迟
	 * 因为操作符需要对其输入流为了生成一个一致性的快照点。简单数据流（没有分区）的延迟增加可以忽略不计
	 * 对于有分区的简单数据流来说，平均延迟也会保证很小，但是最慢的记录通常有较高的延迟
	 */
	EXACTLY_ONCE,

	/**
	 * Sets the checkpointing mode to "at least once". This mode means that the system will
	 * checkpoint the operator and user function state in a simpler way. Upon failure and recovery,
	 * some records may be reflected multiple times in the operator state.
	 *
	 * <p>For example, if a user function counts the number of elements in a stream,
	 * this number will equal to, or larger, than the actual number of elements in the stream,
	 * in the presence of failure and recovery.</p>
	 *
	 * <p>This mode has minimal impact on latency and may be preferable in very-low latency
	 * scenarios, where a sustained very-low latency (such as few milliseconds) is needed,
	 * and where occasional duplicate messages (on recovery) do not matter.</p>
	 *
	 * 此模式对延迟的影响最小，在延迟敏感的情况下可能更为可取，在这种情况下需要持续的极低延迟（例如几毫秒）
	 * 并且偶尔的重复消息（恢复时）无关紧要
	 */
	AT_LEAST_ONCE
}
