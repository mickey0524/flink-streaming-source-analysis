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

package org.apache.flink.streaming.api.functions.source;

import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.TimestampAssigner;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.io.Serializable;

/**
 * Base interface for all stream data sources in Flink. The contract of a stream source
 * is the following: When the source should start emitting elements, the {@link #run} method
 * is called with a {@link SourceContext} that can be used for emitting elements.
 * The run method can run for as long as necessary. The source must, however, react to an
 * invocation of {@link #cancel()} by breaking out of its main loop.
 *
 * <h3>CheckpointedFunction Sources</h3>
 *
 * <p>Sources that also implement the {@link org.apache.flink.streaming.api.checkpoint.CheckpointedFunction}
 * interface must ensure that state checkpointing, updating of internal state and emission of
 * elements are not done concurrently. This is achieved by using the provided checkpointing lock
 * object to protect update of state and emission of elements in a synchronized block.
 *
 * <p>This is the basic pattern one should follow when implementing a checkpointed source:
 *
 * <pre>{@code
 *  public class ExampleCountSource implements SourceFunction<Long>, CheckpointedFunction {
 *      private long count = 0L;
 *      private volatile boolean isRunning = true;
 *
 *      private transient ListState<Long> checkpointedCount;
 *
 *      public void run(SourceContext<T> ctx) {
 *          while (isRunning && count < 1000) {
 *              // this synchronized block ensures that state checkpointing,
 *              // internal state updates and emission of elements are an atomic operation
 *              synchronized (ctx.getCheckpointLock()) {
 *                  ctx.collect(count);
 *                  count++;
 *              }
 *          }
 *      }
 *
 *      public void cancel() {
 *          isRunning = false;
 *      }
 *
 *      public void initializeState(FunctionInitializationContext context) {
 *          this.checkpointedCount = context
 *              .getOperatorStateStore()
 *              .getListState(new ListStateDescriptor<>("count", Long.class));
 *
 *          if (context.isRestored()) {
 *              for (Long count : this.checkpointedCount.get()) {
 *                  this.count = count;
 *              }
 *          }
 *      }
 *
 *      public void snapshotState(FunctionSnapshotContext context) {
 *          this.checkpointedCount.clear();
 *          this.checkpointedCount.add(count);
 *      }
 * }
 * }</pre>
 *
 *
 * <h3>Timestamps and watermarks:</h3>
 * Sources may assign timestamps to elements and may manually emit watermarks.
 * However, these are only interpreted if the streaming program runs on
 * {@link TimeCharacteristic#EventTime}. On other time characteristics
 * ({@link TimeCharacteristic#IngestionTime} and {@link TimeCharacteristic#ProcessingTime}),
 * the watermarks from the source function are ignored.
 *
 * <h3>Gracefully Stopping Functions</h3>
 * Functions may additionally implement the {@link org.apache.flink.api.common.functions.StoppableFunction}
 * interface. "Stopping" a function, in contrast to "canceling" means a graceful exit that leaves the
 * state and the emitted elements in a consistent state.
 *
 * <p>When a source is stopped, the executing thread is not interrupted, but expected to leave the
 * {@link #run(SourceContext)} method in reasonable time on its own, preserving the atomicity
 * of state updates and element emission.
 *
 * @param <T> The type of the elements produced by this source.
 *
 * @see org.apache.flink.api.common.functions.StoppableFunction
 * @see org.apache.flink.streaming.api.TimeCharacteristic
 */
/**
 * Flink 所有数据源头的基础接口，所有数据源的运作如下：当数据源开始 emit 数据的时候
 * run 方法被调用，run 方法可能运行非常长的时间，数据源要能够对 cancel 方法作出反应
 * break 出子循环
 *
 * 实现了 CheckpointedFunction 接口的 Source 要保证状态检查、内部状态更新以及事件的 emit
 * 不并发执行，使用 checkpointing lock 实现
 *
 * 当流处理程序运行在 EventTime 模式下，Source 会给事件设置 ts 或者 emit watermark
 *
 * 优雅结束函数
 * Functions 可能实现了 StoppableFunction 接口，Stopping 是一个函数，与 canceling 方法
 * 相对应，会将状态保存好再退出
 */
@Public
public interface SourceFunction<T> extends Function, Serializable {

	/**
	 * Starts the source. Implementations can use the {@link SourceContext} emit
	 * elements.
	 *
	 * <p>Sources that implement {@link org.apache.flink.streaming.api.checkpoint.CheckpointedFunction}
	 * must lock on the checkpoint lock (using a synchronized block) before updating internal
	 * state and emitting elements, to make both an atomic operation:
	 *
	 * <pre>{@code
	 *  public class ExampleCountSource implements SourceFunction<Long>, CheckpointedFunction {
	 *      private long count = 0L;
	 *      private volatile boolean isRunning = true;
	 *
	 *      private transient ListState<Long> checkpointedCount;
	 *
	 *      public void run(SourceContext<T> ctx) {
	 *          while (isRunning && count < 1000) {
	 *              // this synchronized block ensures that state checkpointing,
	 *              // internal state updates and emission of elements are an atomic operation
	 *              synchronized (ctx.getCheckpointLock()) {
	 *                  ctx.collect(count);
	 *                  count++;
	 *              }
	 *          }
	 *      }
	 *
	 *      public void cancel() {
	 *          isRunning = false;
	 *      }
	 *
	 *      public void initializeState(FunctionInitializationContext context) {
	 *          this.checkpointedCount = context
	 *              .getOperatorStateStore()
	 *              .getListState(new ListStateDescriptor<>("count", Long.class));
	 *
	 *          if (context.isRestored()) {
	 *              for (Long count : this.checkpointedCount.get()) {
	 *                  this.count = count;
	 *              }
	 *          }
	 *      }
	 *
	 *      public void snapshotState(FunctionSnapshotContext context) {
	 *          this.checkpointedCount.clear();
	 *          this.checkpointedCount.add(count);
	 *      }
	 * }
	 * }</pre>
	 *
	 * @param ctx The context to emit elements to and for accessing locks.
	 */
	/**
	 * 开启一个流，通过 SourceContext 来 emit 数据
	 */
	void run(SourceContext<T> ctx) throws Exception;

	/**
	 * Cancels the source. Most sources will have a while loop inside the
	 * {@link #run(SourceContext)} method. The implementation needs to ensure that the
	 * source will break out of that loop after this method is called.
	 *
	 * <p>A typical pattern is to have an {@code "volatile boolean isRunning"} flag that is set to
	 * {@code false} in this method. That flag is checked in the loop condition.
	 *
	 * <p>When a source is canceled, the executing thread will also be interrupted
	 * (via {@link Thread#interrupt()}). The interruption happens strictly after this
	 * method has been called, so any interruption handler can rely on the fact that
	 * this method has completed. It is good practice to make any flags altered by
	 * this method "volatile", in order to guarantee the visibility of the effects of
	 * this method to any interruption handler.
	 */
	/**
	 * 关闭流，大多数流都会有一个 while 循环在 run 方法里，cancel 方法需要保证 while 方法
	 * 能够 break 循环
	 *
	 * 一个非常典型的做法是设置一个字段 "volatile boolean isRunning" 在 run 方法的循环中判断 isRunning
	 * 如果 isRunning 为 false，则 break
	 */
	void cancel();

	// ------------------------------------------------------------------------
	//  source context
	// ------------------------------------------------------------------------

	/**
	 * Interface that source functions use to emit elements, and possibly watermarks.
	 *
	 * @param <T> The type of the elements produced by the source.
	 */
	/**
	 * 源程序用来 emit 数据的接口，可能是 watermarks
	 */
	@Public // Interface might be extended in the future with additional methods.
	interface SourceContext<T> {

		/**
		 * Emits one element from the source, without attaching a timestamp. In most cases,
		 * this is the default way of emitting elements.
		 *
		 * <p>The timestamp that the element will get assigned depends on the time characteristic of
		 * the streaming program:
		 * <ul>
		 *     <li>On {@link TimeCharacteristic#ProcessingTime}, the element has no timestamp.</li>
		 *     <li>On {@link TimeCharacteristic#IngestionTime}, the element gets the system's
		 *         current time as the timestamp.</li>
		 *     <li>On {@link TimeCharacteristic#EventTime}, the element will have no timestamp initially.
		 *         It needs to get a timestamp (via a {@link TimestampAssigner}) before any time-dependent
		 *         operation (like time windows).</li>
		 * </ul>
		 *
		 * @param element The element to emit
		 */
		/**
		 * emit 一个 element，不带 ts，在大多数 case 中，这是默认的 emit element 的方式
		 */
		void collect(T element);

		/**
		 * Emits one element from the source, and attaches the given timestamp. This method
		 * is relevant for programs using {@link TimeCharacteristic#EventTime}, where the
		 * sources assign timestamps themselves, rather than relying on a {@link TimestampAssigner}
		 * on the stream.
		 *
		 * <p>On certain time characteristics, this timestamp may be ignored or overwritten.
		 * This allows programs to switch between the different time characteristics and behaviors
		 * without changing the code of the source functions.
		 * <ul>
		 *     <li>On {@link TimeCharacteristic#ProcessingTime}, the timestamp will be ignored,
		 *         because processing time never works with element timestamps.</li>
		 *     <li>On {@link TimeCharacteristic#IngestionTime}, the timestamp is overwritten with the
		 *         system's current time, to realize proper ingestion time semantics.</li>
		 *     <li>On {@link TimeCharacteristic#EventTime}, the timestamp will be used.</li>
		 * </ul>
		 *
		 * @param element The element to emit
		 * @param timestamp The timestamp in milliseconds since the Epoch
		 */
		/**
		 * EventTime 模式使用的方法，element 自带时间戳，不依赖 TimestampAssigner 获取 ts
		 * 在 ProcessingTime 模式下，本方法会被 ignore
		 * 在 IngestionTime 模式下，本方法会被重写为使用机器当前的时间
		 */
		@PublicEvolving
		void collectWithTimestamp(T element, long timestamp);

		/**
		 * Emits the given {@link Watermark}. A Watermark of value {@code t} declares that no
		 * elements with a timestamp {@code t' <= t} will occur any more. If further such
		 * elements will be emitted, those elements are considered <i>late</i>.
		 *
		 * <p>This method is only relevant when running on {@link TimeCharacteristic#EventTime}.
		 * On {@link TimeCharacteristic#ProcessingTime},Watermarks will be ignored. On
		 * {@link TimeCharacteristic#IngestionTime}, the Watermarks will be replaced by the
		 * automatic ingestion time watermarks.
		 *
		 * @param mark The Watermark to emit
		 */
		/**
		 * EventTime 模式使用的方法，emit watermark，宣布之后没有 ts <= mark.ts 的元素
		 * 如果有的话，这些元素被认为是 late
		 */
		@PublicEvolving
		void emitWatermark(Watermark mark);

		/**
		 * Marks the source to be temporarily idle. This tells the system that this source will
		 * temporarily stop emitting records and watermarks for an indefinite amount of time. This
		 * is only relevant when running on {@link TimeCharacteristic#IngestionTime} and
		 * {@link TimeCharacteristic#EventTime}, allowing downstream tasks to advance their
		 * watermarks without the need to wait for watermarks from this source while it is idle.
		 *
		 * <p>Source functions should make a best effort to call this method as soon as they
		 * acknowledge themselves to be idle. The system will consider the source to resume activity
		 * again once {@link SourceContext#collect(T)}, {@link SourceContext#collectWithTimestamp(T, long)},
		 * or {@link SourceContext#emitWatermark(Watermark)} is called to emit elements or watermarks from the source.
		 */
		@PublicEvolving
		void markAsTemporarilyIdle();

		/**
		 * Returns the checkpoint lock. Please refer to the class-level comment in
		 * {@link SourceFunction} for details about how to write a consistent checkpointed
		 * source.
		 *
		 * @return The object to use as the lock
		 */
		Object getCheckpointLock();

		/**
		 * This method is called by the system to shut down the context.
		 */
		void close();
	}
}
