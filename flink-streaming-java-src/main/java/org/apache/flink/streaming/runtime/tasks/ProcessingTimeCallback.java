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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;

/**
 * Interface for processing-time callbacks that can be registered at a
 * {@link ProcessingTimeService}.
 */
/**
 * ProcessingTimeService 中 ScheduledFuture 的回调函数
 */
@Internal
public interface ProcessingTimeCallback {

	/**
	 * This method is invoked with the timestamp for which the trigger was scheduled.
	 *
	 * <p>If the triggering is delayed for whatever reason (trigger timer was blocked, JVM stalled
	 * due to a garbage collection), the timestamp supplied to this function will still be the
	 * original timestamp for which the trigger was scheduled.
	 *
	 * @param timestamp The timestamp for which the trigger event was scheduled.
	 */
	/**
	 * 如果触发因任何原因而被延迟（触发计时器被阻止，JVM由于垃圾收集而停止）
	 * 则提供给此功能的时间戳仍将是安排触发器的原始时间戳
	 * 具体实现：会保存下注册的时候传递进去的时间戳，然后调用此函数
	 */
	void onProcessingTime(long timestamp) throws Exception;
}
