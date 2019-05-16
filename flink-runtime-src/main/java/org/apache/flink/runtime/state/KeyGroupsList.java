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

/**
 * This interface offers ordered random read access to multiple key group ids.
 */
/**
 * 此接口提供对多个 key group ids 的有序随机读取访问
 */
public interface KeyGroupsList extends Iterable<Integer> {

	/**
	 * Returns the number of key group ids in the list.
	 */
	/**
	 * 获取 key group ids 数量
	 */
	int getNumberOfKeyGroups();

	/**
	 * Returns the id of the keygroup at the given index, where index in interval [0,  {@link #getNumberOfKeyGroups()}[.
	 *
	 * @param idx the index into the list
	 * @return key group id at the given index
	 */
	/**
	 * 返回相应下标的 key group id
	 */
	int getKeyGroupId(int idx);

	/**
	 * Returns true, if the given key group id is contained in the list, otherwise false.
	 */
	/**
	 * 判断给定的 keyGroupId 是否处于 list 中
	 */
	boolean contains(int keyGroupId);
}
