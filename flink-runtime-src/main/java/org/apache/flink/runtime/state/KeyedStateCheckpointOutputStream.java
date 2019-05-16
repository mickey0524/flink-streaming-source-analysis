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
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.Arrays;

/**
 * Checkpoint output stream that allows to write raw keyed state in a partitioned way, split into key-groups.
 */
/**
 * 允许以分区方式写入原始键控状态的检查点输出流，拆分为键组
 */
@PublicEvolving
public final class KeyedStateCheckpointOutputStream extends NonClosingCheckpointOutputStream<KeyGroupsStateHandle> {

	public static final long NO_OFFSET_SET = -1L;  // 当前没有 OffsetSet
	public static final int NO_CURRENT_KEY_GROUP = -1;  // 当前没有 KeyGroup

	private int currentKeyGroup;
	private final KeyGroupRangeOffsets keyGroupRangeOffsets;

	public KeyedStateCheckpointOutputStream(
			CheckpointStreamFactory.CheckpointStateOutputStream delegate, KeyGroupRange keyGroupRange) {

		super(delegate);
		Preconditions.checkNotNull(keyGroupRange);
		Preconditions.checkArgument(keyGroupRange != KeyGroupRange.EMPTY_KEY_GROUP_RANGE);

		this.currentKeyGroup = NO_CURRENT_KEY_GROUP;  // 当前的 KeyGroup
		long[] emptyOffsets = new long[keyGroupRange.getNumberOfKeyGroups()];  // 有多少 keyGroup，创建多大的 offset 数组
		// mark offsets as currently not set
		Arrays.fill(emptyOffsets, NO_OFFSET_SET);
		this.keyGroupRangeOffsets = new KeyGroupRangeOffsets(keyGroupRange, emptyOffsets);
	}

	@Override
	public void close() throws IOException {
		// users should not be able to actually close the stream, it is closed by the system.
		// TODO if we want to support async writes, this call could trigger a callback to the snapshot context that a handle is available.
	}

	/**
	 * Returns a list of all key-groups which can be written to this stream.
	 */
	/**
	 * 返回可以写入此流的所有键组的列表
	 */
	public KeyGroupsList getKeyGroupList() {
		return keyGroupRangeOffsets.getKeyGroupRange();
	}

	/**
	 * User code can call this method to signal that it begins to write a new key group with the given key group id.
	 * This id must be within the {@link KeyGroupsList} provided by the stream. Each key-group can only be started once
	 * and is considered final/immutable as soon as this method is called again.
	 */
	/**
	 * 用户代码可以调用此方法来表示它开始使用给定的 keyGroupId 编写新的 key group
	 * 此 id 必须位于流提供的 KeyGroupsList 中
	 * 每个键组只能启动一次，并且只要再次调用此方法就被视为 final/immutable
	 */
	public void startNewKeyGroup(int keyGroupId) throws IOException {
		if (isKeyGroupAlreadyStarted(keyGroupId)) {
			throw new IOException("Key group " + keyGroupId + " already registered!");
		}
		keyGroupRangeOffsets.setKeyGroupOffset(keyGroupId, delegate.getPos());
		currentKeyGroup = keyGroupId;
	}

	/**
	 * Returns true, if the key group with the given id was already started. The key group might not yet be finished,
	 * if it's id is equal to the return value of {@link #getCurrentKeyGroup()}.
	 */
	/**
	 * 如果 key group 已经启动了，返回 true
	 */
	public boolean isKeyGroupAlreadyStarted(int keyGroupId) {
		return NO_OFFSET_SET != keyGroupRangeOffsets.getKeyGroupOffset(keyGroupId);
	}

	/**
	 * Returns true if the key group is already completely written and immutable. It was started and since then another
	 * key group has been started.
	 */
	/**
	 * 是否 key group 已经完成了
	 */
	public boolean isKeyGroupAlreadyFinished(int keyGroupId) {
		return isKeyGroupAlreadyStarted(keyGroupId) && keyGroupId != getCurrentKeyGroup();
	}

	/**
	 * Returns the key group that is currently being written. The key group was started but not yet finished, i.e. data
	 * can still be added. If no key group was started, this returns {@link #NO_CURRENT_KEY_GROUP}.
	 */
	public int getCurrentKeyGroup() {
		return currentKeyGroup;
	}

	@Override
	KeyGroupsStateHandle closeAndGetHandle() throws IOException {
		StreamStateHandle streamStateHandle = delegate.closeAndGetHandle();
		return streamStateHandle != null ? new KeyGroupsStateHandle(keyGroupRangeOffsets, streamStateHandle) : null;
	}
}