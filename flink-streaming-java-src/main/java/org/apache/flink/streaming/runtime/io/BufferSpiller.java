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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.StringUtils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The buffer spiller takes the buffers and events from a data stream and adds them to a spill file.
 * After a number of elements have been spilled, the spiller can "roll over": It presents the spilled
 * elements as a readable sequence, and opens a new spill file.
 *
 * <p>This implementation buffers data effectively in the OS cache, which gracefully extends to the
 * disk. Most data is written and re-read milliseconds later. The file is deleted after the read.
 * Consequently, in most cases, the data will never actually hit the physical disks.
 *
 * <p>IMPORTANT: The SpilledBufferOrEventSequences created by this spiller all reuse the same
 * reading memory (to reduce overhead) and can consequently not be read concurrently.
 */
/**
 * BufferSpiller 从数据流中获取缓冲区和事件，并将它们添加到溢出文件中
 * 在溢出了一定数量的元素之后，spiller 进行翻转操作：它将溢出的元素表现为可读的序列
 * 并打开一个新的 spill 文件
 * 
 * 此实现有效地在 OS 缓存中缓冲数据，缓存优雅地扩展到磁盘
 * 大多数数据都是在写入后几毫秒就重新读取，读取后删除该文件
 * 因此，在大多数情况下，数据实际上永远不会到达物理磁盘
 * 
 * 需要注意的是：由此 spiller 创建的 SpilledBufferOrEventSequences 都重用相同的读取内存（以减少开销）
 * 因此无法同时读取
 */
@Internal
@Deprecated
public class BufferSpiller implements BufferBlocker {

	/** Size of header in bytes (see add method). */
	// header 字节数 (add 方法有用)
	static final int HEADER_SIZE = 9;

	/** The counter that selects the next directory to spill into. */
	// 选择下一个 spill 的目录的计数器
	private static final AtomicInteger DIRECTORY_INDEX = new AtomicInteger(0);

	/** The size of the buffer with which data is read back in. */
	// 读回数据的缓冲区大小
	private static final int READ_BUFFER_SIZE = 1024 * 1024;

	/** The directories to spill to. */
	// spill 的目录
	private final File tempDir;

	/** The name prefix for spill files. */
	// spill 文件名前缀
	private final String spillFilePrefix;

	/** The buffer used for bulk reading data (used in the SpilledBufferOrEventSequence). */
	// 用于批量读取数据的缓冲区（用于 SpilledBufferOrEventSequence）
	private final ByteBuffer readBuffer;

	/** The buffer that encodes the spilled header. */
	// 编码溢出标头的缓冲区
	private final ByteBuffer headBuffer;

	/** The file that we currently spill to. */
	// 我们当前 spill 的文件
	private File currentSpillFile;

	/** The channel of the file we currently spill to. */
	// 我们当前 spill 的文件 channel
	private FileChannel currentChannel;

	/** The page size, to let this reader instantiate properly sized memory segments. */
	// 页面大小，让这个阅读器实例化适当大小的内存段
	private final int pageSize;

	/** A counter, to created numbered spill files. */
	// 一个计数器，用于创建溢出文件的编号
	private int fileCounter;

	/** The number of bytes written since the last roll over. */
	// 从上次反转以来，写入的字节数目
	private long bytesWritten;

	/**
	 * Creates a new buffer spiller, spilling to one of the I/O manager's temp directories.
	 *
	 * @param ioManager The I/O manager for access to the temp directories.
	 * @param pageSize The page size used to re-create spilled buffers.
	 * @throws IOException Thrown if the temp files for spilling cannot be initialized.
	 */
	/**
	 * 创建一个新的缓冲区 spiller，溢出到 I/O 管理器的临时目录之一
	 */
	public BufferSpiller(IOManager ioManager, int pageSize) throws IOException {
		this.pageSize = pageSize;

		// 分配缓冲区大小
		this.readBuffer = ByteBuffer.allocateDirect(READ_BUFFER_SIZE);
		this.readBuffer.order(ByteOrder.LITTLE_ENDIAN);

		this.headBuffer = ByteBuffer.allocateDirect(16);
		this.headBuffer.order(ByteOrder.LITTLE_ENDIAN);

		// 获取 spill 的目录
		File[] tempDirs = ioManager.getSpillingDirectories();
		this.tempDir = tempDirs[DIRECTORY_INDEX.getAndIncrement() % tempDirs.length];

		// 随机数生成一个前缀
		byte[] rndBytes = new byte[32];
		ThreadLocalRandom.current().nextBytes(rndBytes);
		this.spillFilePrefix = StringUtils.byteToHexString(rndBytes) + '.';

		// prepare for first contents
		// 创建 file 和 channel
		createSpillingChannel();
	}

	/**
	 * Adds a buffer or event to the sequence of spilled buffers and events.
	 *
	 * @param boe The buffer or event to add and spill.
	 * @throws IOException Thrown, if the buffer of event could not be spilled.
	 */
	/**
	 * 添加一个 Buffer 或者 Event
	 */
	@Override
	public void add(BufferOrEvent boe) throws IOException {
		try {
			ByteBuffer contents;
			if (boe.isBuffer()) {
				Buffer buf = boe.getBuffer();
				contents = buf.getNioBufferReadable();
			}
			else {
				contents = EventSerializer.toSerializedEvent(boe.getEvent());
			}

			headBuffer.clear();
			headBuffer.putInt(boe.getChannelIndex());  // 4 字节
			headBuffer.putInt(contents.remaining());  // 4 字节
			headBuffer.put((byte) (boe.isBuffer() ? 0 : 1));  // 1 字节，加起来 9 个字节，对应 HEADER_SIZE
			headBuffer.flip();

			bytesWritten += (headBuffer.remaining() + contents.remaining());

			FileUtils.writeCompletely(currentChannel, headBuffer);
			FileUtils.writeCompletely(currentChannel, contents);
		}
		finally {
			if (boe.isBuffer()) {
				boe.getBuffer().recycleBuffer();
			}
		}
	}

	/**
	 * NOTE: The BufferOrEventSequences created by this method all reuse the same reading memory
	 * (to reduce overhead) and can consequently not be read concurrently with each other.
	 *
	 * <p>To create a sequence that can be read concurrently with the previous BufferOrEventSequence,
	 * use the {@link #rollOverWithoutReusingResources()} ()} method.
	 *
	 * @return The readable sequence of spilled buffers and events, or 'null', if nothing was added.
	 * @throws IOException Thrown, if the readable sequence could not be created, or no new spill
	 *                     file could be created.
	 */
	/**
	 * 由此方法创建的 BufferOrEventSequences 都重用相同的读取内存（以减少开销），因此无法彼此同时读取
	 * 
	 * 要创建可以与之前的 BufferOrEventSequence 同时读取的序列
	 * 请使用 rollOverWithoutReusingResources() 方法
	 */
	@Override
	public BufferOrEventSequence rollOverReusingResources() throws IOException {
		return rollOver(false);
	}

	/**
	 * The BufferOrEventSequence returned by this method is safe for concurrent consumption with
	 * any previously returned sequence.
	 *
	 * @return The readable sequence of spilled buffers and events, or 'null', if nothing was added.
	 * @throws IOException Thrown, if the readable sequence could not be created, or no new spill
	 *                     file could be created.
	 */
	/**
	 * 此方法返回的 BufferOrEventSequence 对于任何先前返回的序列的并发消耗是安全的
	 */
	@Override
	public BufferOrEventSequence rollOverWithoutReusingResources() throws IOException {
		return rollOver(true);
	}

	private BufferOrEventSequence rollOver(boolean newBuffer) throws IOException {
		if (bytesWritten == 0) {
			return null;
		}

		ByteBuffer buf;
		// 如果 newBuffer 为 true，说明不能重用资源，需要重新分配
		if (newBuffer) {
			buf = ByteBuffer.allocateDirect(READ_BUFFER_SIZE);
			buf.order(ByteOrder.LITTLE_ENDIAN);
		} else {
			buf = readBuffer;
		}

		// create a reader for the spilled data
		// 为溢出的数据创建一个 reader
		currentChannel.position(0L);
		SpilledBufferOrEventSequence seq =
				new SpilledBufferOrEventSequence(currentSpillFile, currentChannel, buf, pageSize);

		// create ourselves a new spill file
		// spill 之后，新开一个 spill 文件
		createSpillingChannel();

		bytesWritten = 0L;
		return seq;
	}

	/**
	 * Cleans up the current spilling channel and file.
	 *
	 * <p>Does not clean up the SpilledBufferOrEventSequences generated by calls to
	 * {@link #rollOver(boolean false)}.
	 *
	 * @throws IOException Thrown if channel closing or file deletion fail.
	 */
	/**
	 * 清空当前 spill 的 channel 和 file
	 */
	@Override
	public void close() throws IOException {
		currentChannel.close();
		if (!currentSpillFile.delete()) {
			throw new IOException("Cannot delete spill file");
		}
	}

	/**
	 * Gets the number of bytes written in the current spill file.
	 *
	 * @return the number of bytes written in the current spill file
	 */
	@Override
	public long getBytesBlocked() {
		return bytesWritten;
	}

	// ------------------------------------------------------------------------
	//  For testing
	// ------------------------------------------------------------------------

	File getCurrentSpillFile() {
		return currentSpillFile;
	}

	FileChannel getCurrentChannel() {
		return currentChannel;
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	@SuppressWarnings("resource")
	private void createSpillingChannel() throws IOException {
		// 创建当前目录下的 spill 文件
		currentSpillFile = new File(tempDir, spillFilePrefix + (fileCounter++) + ".buffer");
		// 获取当前的 channel
		currentChannel = new RandomAccessFile(currentSpillFile, "rw").getChannel();
	}

	// ------------------------------------------------------------------------

	/**
	 * This class represents a sequence of spilled buffers and events, created by the
	 * {@link BufferSpiller}.
	 */
	/**
	 * 此类表示由 BufferSpiller 创建的一系列溢出缓冲区和事件
	 */
	@Deprecated
	public static class SpilledBufferOrEventSequence implements BufferOrEventSequence {

		/** Header is "channel index" (4 bytes) + length (4 bytes) + buffer/event (1 byte). */
		// 4 字节的 channel 索引，4 字节的长度以及 1 字节的类型（是 Buffer 还是 Event）
		private static final int HEADER_LENGTH = 9;

		/** The file containing the data. */
		// 包含数据的 File
		private final File file;

		/** The file channel to draw the data from. */
		// 从中提取数据的文件通道
		private final FileChannel fileChannel;

		/** The byte buffer for bulk reading. */
		// 用于批量读取的字节缓冲区
		private final ByteBuffer buffer;

		/** We store this size as a constant because it is crucial it never changes. */
		// 我们将此大小存储为常量，因为它永远不会改变
		private final long size;

		/** The page size to instantiate properly sized memory segments. */
		// 用于实例化正确大小的内存段的页面大小
		private final int pageSize;

		/** Flag to track whether the sequence has been opened already. */
		// 用于跟踪序列是否已打开的标记
		private boolean opened = false;

		/**
		 * Create a reader that reads a sequence of spilled buffers and events.
		 *
		 * @param file The file with the data.
		 * @param fileChannel The file channel to read the data from.
		 * @param buffer The buffer used for bulk reading.
		 * @param pageSize The page size to use for the created memory segments.
		 */
		SpilledBufferOrEventSequence(File file, FileChannel fileChannel, ByteBuffer buffer, int pageSize)
				throws IOException {
			this.file = file;
			this.fileChannel = fileChannel;
			this.buffer = buffer;
			this.pageSize = pageSize;
			this.size = fileChannel.size();
		}

		/**
		 * This method needs to be called before the first call to {@link #getNext()}.
		 * Otherwise the results of {@link #getNext()} are not predictable.
		 */
		/**
		 * 需要在第一次调用 getNext() 之前调用此方法
		 * 否则 getNext() 的结果是不可预测的
		 */
		@Override
		public void open() {
			if (!opened) {
				opened = true;
				buffer.position(0);
				buffer.limit(0);
			}
		}

		@Override
		public BufferOrEvent getNext() throws IOException {
			if (buffer.remaining() < HEADER_LENGTH) {
				buffer.compact();

				while (buffer.position() < HEADER_LENGTH) {
					// 从 fileChannel 中读取数据到 buffer 中
					if (fileChannel.read(buffer) == -1) {
						if (buffer.position() == 0) {
							// no trailing data
							// 没有数据
							return null;
						} else {
							throw new IOException("Found trailing incomplete buffer or event");
						}
					}
				}

				buffer.flip();
			}
			
			// 读取 9 字节的 header
			final int channel = buffer.getInt();
			final int length = buffer.getInt();
			final boolean isBuffer = buffer.get() == 0;

			if (isBuffer) {
				// deserialize buffer
				// 反序列化 buffer
				if (length > pageSize) {
					throw new IOException(String.format(
							"Spilled buffer (%d bytes) is larger than page size of (%d bytes)", length, pageSize));
				}

				MemorySegment seg = MemorySegmentFactory.allocateUnpooledSegment(pageSize);

				int segPos = 0;
				int bytesRemaining = length;
				
				// 循环读取，将 content 从 buffer 中读取到内存段中
				while (true) {
					int toCopy = Math.min(buffer.remaining(), bytesRemaining);
					if (toCopy > 0) {
						seg.put(segPos, buffer, toCopy);
						segPos += toCopy;
						bytesRemaining -= toCopy;
					}

					if (bytesRemaining == 0) {
						break;
					}
					else {
						buffer.clear();
						if (fileChannel.read(buffer) == -1) {
							throw new IOException("Found trailing incomplete buffer");
						}
						buffer.flip();
					}
				}

				Buffer buf = new NetworkBuffer(seg, FreeingBufferRecycler.INSTANCE);
				buf.setSize(length);

				return new BufferOrEvent(buf, channel);
			}
			else {
				// deserialize event
				// 反序列化事件
				if (length > buffer.capacity() - HEADER_LENGTH) {
					throw new IOException("Event is too large");
				}

				if (buffer.remaining() < length) {
					buffer.compact();

					while (buffer.position() < length) {
						if (fileChannel.read(buffer) == -1) {
							throw new IOException("Found trailing incomplete event");
						}
					}

					buffer.flip();
				}

				int oldLimit = buffer.limit();
				buffer.limit(buffer.position() + length);
				AbstractEvent evt = EventSerializer.fromSerializedEvent(buffer, getClass().getClassLoader());
				buffer.limit(oldLimit);

				return new BufferOrEvent(evt, channel);
			}
		}

		@Override
		public void cleanup() throws IOException {
			fileChannel.close();
			if (!file.delete()) {
				throw new IOException("Cannot remove temp file for stream alignment writer");
			}
		}

		@Override
		public long size() {
			return size;
		}
	}
}
