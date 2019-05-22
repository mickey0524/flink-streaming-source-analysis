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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.io.PostVersionedIOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Serialization proxy for the timer services for a given key-group.
 */
/**
 * 用于给定 key-group 的计时器服务的序列化代理
 */
@Internal
public class InternalTimerServiceSerializationProxy<K> extends PostVersionedIOReadableWritable {

	public static final int VERSION = 2;

	/** The key-group timer services to write / read. */
	private final InternalTimeServiceManager<K> timerServicesManager;

	/** The user classloader; only relevant if the proxy is used to restore timer services. */
	private ClassLoader userCodeClassLoader;

	/** Properties of restored timer services. */
	private final int keyGroupIdx;


	/**
	 * Constructor to use when restoring timer services.
	 */
	/**
	 * 恢复时间服务时候使用的构造器
	 */
	public InternalTimerServiceSerializationProxy(
		InternalTimeServiceManager<K> timerServicesManager,
		ClassLoader userCodeClassLoader,
		int keyGroupIdx) {
		this.timerServicesManager = checkNotNull(timerServicesManager);
		this.userCodeClassLoader = checkNotNull(userCodeClassLoader);
		this.keyGroupIdx = keyGroupIdx;
	}

	/**
	 * Constructor to use when writing timer services.
	 */
	/**
	 * 存储时间服务的时候使用的构造器
	 */
	public InternalTimerServiceSerializationProxy(
		InternalTimeServiceManager<K> timerServicesManager,
		int keyGroupIdx) {
		this.timerServicesManager = checkNotNull(timerServicesManager);
		this.keyGroupIdx = keyGroupIdx;
	}

	/**
	 * 获取 version
	 */
	@Override
	public int getVersion() {
		return VERSION;
	}

	@Override
	public int[] getCompatibleVersions() {
		return new int[] { VERSION, 1 };
	}

	@Override
	@SuppressWarnings("unchecked")
	public void write(DataOutputView out) throws IOException {
		// 写入版本信息
		super.write(out);
		// 从 timerServicesManager 中获取所有的定时器服务
		final Map<String, InternalTimerServiceImpl<K, ?>> registeredTimerServices =
			timerServicesManager.getRegisteredTimerServices();

		// 写入时间服务个数
		out.writeInt(registeredTimerServices.size());
		for (Map.Entry<String, InternalTimerServiceImpl<K, ?>> entry : registeredTimerServices.entrySet()) {
			String serviceName = entry.getKey();
			InternalTimerServiceImpl<K, ?> timerService = entry.getValue();

			out.writeUTF(serviceName);  // 写入定时器名称
			// 将进程时间定时器和事件时间定时器写入快照
			InternalTimersSnapshotReaderWriters
				.getWriterForVersion(
					VERSION,
					timerService.snapshotTimersForKeyGroup(keyGroupIdx),
					timerService.getKeySerializer(),
					(TypeSerializer) timerService.getNamespaceSerializer())
				.writeTimersSnapshot(out);
		}
	}

	@Override
	protected void read(DataInputView in, boolean wasVersioned) throws IOException {
		// 读取时间服务的个数
		int noOfTimerServices = in.readInt();

		for (int i = 0; i < noOfTimerServices; i++) {
			// 读取时间服务的名称
			String serviceName = in.readUTF();

			int readerVersion = wasVersioned ? getReadVersion() : InternalTimersSnapshotReaderWriters.NO_VERSION;
			InternalTimersSnapshot<?, ?> restoredTimersSnapshot = InternalTimersSnapshotReaderWriters
				.getReaderForVersion(readerVersion, userCodeClassLoader)
				.readTimersSnapshot(in);

			InternalTimerServiceImpl<K, ?> timerService = registerOrGetTimerService(
				serviceName,
				restoredTimersSnapshot);
			
			// 根据 restoredTimersSnapshot 快照恢复
			timerService.restoreTimersForKeyGroup(restoredTimersSnapshot, keyGroupIdx);
		}
	}

	@SuppressWarnings("unchecked")
	// 在 timerServicesManager 中创建一个 InternalTimerServiceImpl
	private <N> InternalTimerServiceImpl<K, N> registerOrGetTimerService(
		String serviceName, InternalTimersSnapshot<?, ?> restoredTimersSnapshot) {
		final TypeSerializer<K> keySerializer = (TypeSerializer<K>) restoredTimersSnapshot.getKeySerializerSnapshot().restoreSerializer();
		final TypeSerializer<N> namespaceSerializer = (TypeSerializer<N>) restoredTimersSnapshot.getNamespaceSerializerSnapshot().restoreSerializer();
		TimerSerializer<K, N> timerSerializer = new TimerSerializer<>(keySerializer, namespaceSerializer);
		return timerServicesManager.registerOrGetTimerService(serviceName, timerSerializer);
	}
}
