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

package org.apache.flink.runtime.state;

import org.apache.flink.annotation.PublicEvolving;

/**
 * This interface provides a context in which user functions that use managed state (i.e. state that is managed by state
 * backends) can participate in a snapshot. As snapshots of the backends themselves are taken by the system, this
 * interface mainly provides meta information about the checkpoint.
 */
/**
 * 此接口提供一个上下文，其中使用 managed state 的用户函数（即由状态后端管理的状态）可以参与快照
 * 由于系统采用后端本身的快照，因此该接口主要提供有关检查点的元信息
 */
@PublicEvolving
public interface FunctionSnapshotContext extends ManagedSnapshotContext {
}
