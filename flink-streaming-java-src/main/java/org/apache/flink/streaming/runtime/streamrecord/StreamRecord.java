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

package org.apache.flink.streaming.runtime.streamrecord;

import org.apache.flink.annotation.Internal;

/**
 * One value in a data stream. This stores the value and an optional associated timestamp.
 *
 * @param <T> The type encapsulated with the stream record.
 */
/**
 * 数据流中的一个 value，存储 value 和一个可选的相关的时间戳
 */
@Internal
public final class StreamRecord<T> extends StreamElement {

	/** The actual value held by this record. */
	// record 真正的 value
	private T value;

	/** The timestamp of the record. */
	// record 的时间戳 
	private long timestamp;

	/** Flag whether the timestamp is actually set. */
	// 是否设置了时间戳
	private boolean hasTimestamp;

	/**
	 * Creates a new StreamRecord. The record does not have a timestamp.
	 */
	public StreamRecord(T value) {
		this.value = value;
	}

	/**
	 * Creates a new StreamRecord wrapping the given value. The timestamp is set to the
	 * given timestamp.
	 *
	 * @param value The value to wrap in this {@link StreamRecord}
	 * @param timestamp The timestamp in milliseconds
	 */
	public StreamRecord(T value, long timestamp) {
		this.value = value;
		this.timestamp = timestamp;
		this.hasTimestamp = true;
	}

	// ------------------------------------------------------------------------
	//  Accessors
	// ------------------------------------------------------------------------

	/**
	 * Returns the value wrapped in this stream value.
	 */
	public T getValue() {
		return value;
	}

	/**
	 * Returns the timestamp associated with this stream value in milliseconds.
	 */
	public long getTimestamp() {
		if (hasTimestamp) {
			return timestamp;
		} else {
			return Long.MIN_VALUE;
//			throw new IllegalStateException(
//					"Record has no timestamp. Is the time characteristic set to 'ProcessingTime', or " +
//							"did you forget to call 'DataStream.assignTimestampsAndWatermarks(...)'?");
		}
	}

	/** Checks whether this record has a timestamp.
	 *
 	 * @return True if the record has a timestamp, false if not.
	 */
	public boolean hasTimestamp() {
		return hasTimestamp;
	}

	// ------------------------------------------------------------------------
	//  Updating
	// ------------------------------------------------------------------------

	/**
	 * Replace the currently stored value by the given new value. This returns a StreamElement
	 * with the generic type parameter that matches the new value while keeping the old
	 * timestamp.
	 *
	 * @param element Element to set in this stream value
	 * @return Returns the StreamElement with replaced value
	 */
	@SuppressWarnings("unchecked")
	public <X> StreamRecord<X> replace(X element) {
		this.value = (T) element;
		return (StreamRecord<X>) this;
	}

	/**
	 * Replace the currently stored value by the given new value and the currently stored
	 * timestamp with the new timestamp. This returns a StreamElement with the generic type
	 * parameter that matches the new value.
	 *
	 * @param value The new value to wrap in this StreamRecord
	 * @param timestamp The new timestamp in milliseconds
	 * @return Returns the StreamElement with replaced value
	 */
	@SuppressWarnings("unchecked")
	public <X> StreamRecord<X> replace(X value, long timestamp) {
		this.timestamp = timestamp;
		this.value = (T) value;
		this.hasTimestamp = true;

		return (StreamRecord<X>) this;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
		this.hasTimestamp = true;
	}

	public void eraseTimestamp() {
		this.hasTimestamp = false;
	}

	// ------------------------------------------------------------------------
	//  Copying
	// ------------------------------------------------------------------------

	/**
	 * Creates a copy of this stream record. Uses the copied value as the value for the new
	 * record, i.e., only copies timestamp fields.
	 */
	public StreamRecord<T> copy(T valueCopy) {
		StreamRecord<T> copy = new StreamRecord<>(valueCopy);
		copy.timestamp = this.timestamp;
		copy.hasTimestamp = this.hasTimestamp;
		return copy;
	}

	/**
	 * Copies this record into the new stream record. Uses the copied value as the value for the new
	 * record, i.e., only copies timestamp fields.
	 */
	public void copyTo(T valueCopy, StreamRecord<T> target) {
		target.value = valueCopy;
		target.timestamp = this.timestamp;
		target.hasTimestamp = this.hasTimestamp;
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	/**
	 * 重写 equals 方法
	 */
	@Override
	public boolean equals(Object o) {
		// 如果相等，直接返回 true
		if (this == o) {
			return true;
		}
		// 判断两个实例的 class 是否相同
		else if (o != null && getClass() == o.getClass()) {
			StreamRecord<?> that = (StreamRecord<?>) o;
			// 判断实例属性是否相同
			return this.hasTimestamp == that.hasTimestamp &&
					(!this.hasTimestamp || this.timestamp == that.timestamp) &&
					(this.value == null ? that.value == null : this.value.equals(that.value));
		}
		else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		int result = value != null ? value.hashCode() : 0;
		return 31 * result + (hasTimestamp ? (int) (timestamp ^ (timestamp >>> 32)) : 0);
	}

	@Override
	public String toString() {
		return "Record @ " + (hasTimestamp ? timestamp : "(undef)") + " : " + value;
	}
}
