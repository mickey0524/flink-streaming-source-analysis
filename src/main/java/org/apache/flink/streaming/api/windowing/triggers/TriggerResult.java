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

package org.apache.flink.streaming.api.windowing.triggers;

/**
 * Result type for trigger methods. This determines what happens with the window,
 * for example whether the window function should be called, or the window
 * should be discarded.
 *
 * <p>If a {@link Trigger} returns {@link #FIRE} or {@link #FIRE_AND_PURGE} but the window does not
 * contain any data the window function will not be invoked, i.e. no data will be produced for the
 * window.
 */
/**
 * trigger 方法的返回类型。这决定了窗口发生了什么
 * 举个例子，窗口函数是否被调用，或者窗口是否应该被丢弃
 *
 * 如果 Trigger 返回 FIRE 或 FIRE_AND_PURGE，但是窗口没有包含任何数据
 * 窗口函数不会被调用
 */
public enum TriggerResult {

	/**
	 * No action is taken on the window.
	 */ 
	/**
	 * 窗口中没有发送任何操作
	 */
	CONTINUE(false, false),

	/**
	 * {@code FIRE_AND_PURGE} evaluates the window function and emits the window
	 * result.
	 */
	FIRE_AND_PURGE(true, true),

	/**
	 * On {@code FIRE}, the window is evaluated and results are emitted.
	 * The window is not purged, though, all elements are retained.
	 */
	FIRE(true, false),

	/**
	 * All elements in the window are cleared and the window is discarded,
	 * without evaluating the window function or emitting any elements.
	 */
	PURGE(false, true);

	// ------------------------------------------------------------------------

	private final boolean fire;  // 是否触发
	private final boolean purge;  // 是否清洗

	TriggerResult(boolean fire, boolean purge) {
		this.purge = purge;
		this.fire = fire;
	}

	public boolean isFire() {
		return fire;
	}

	public boolean isPurge() {
		return purge;
	}
}
