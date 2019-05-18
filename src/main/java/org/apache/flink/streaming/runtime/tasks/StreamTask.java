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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.FileSystemSafetyNet;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.runtime.state.CheckpointStorageWorkerView;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateBackendLoader;
import org.apache.flink.runtime.state.TaskStateManager;
import org.apache.flink.runtime.taskmanager.DispatcherThreadFactory;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFinalizer;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializer;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializerImpl;
import org.apache.flink.streaming.runtime.io.RecordWriterOutput;
import org.apache.flink.streaming.runtime.partitioner.ConfigurableStreamPartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusMaintainer;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Base class for all streaming tasks. A task is the unit of local processing that is deployed
 * and executed by the TaskManagers. Each task runs one or more {@link StreamOperator}s which form
 * the Task's operator chain. Operators that are chained together execute synchronously in the
 * same thread and hence on the same stream partition. A common case for these chains
 * are successive map/flatmap/filter tasks.
 * 
 * 所有流式任务的基类。一个 task 是本地进程的单位，由 TaskManagers 部署和执行
 * 每个 task 执行一个或多个 StreamOperator，这些 StreamOperators 组成了 Task 的操作链
 * 链在一起的 Operators 在相同的线程内同步执行，因此也存在于相同的流分区中
 * 一个典型的 chains case 是 map/flatmap/filter tasks
 *
 * <p>The task chain contains one "head" operator and multiple chained operators.
 * The StreamTask is specialized for the type of the head operator: one-input and two-input tasks,
 * as well as for sources, iteration heads and iteration tails.
 * 
 * task 链包括一个 head operator 和多个链式相连的 operators
 * StreamTask 是这些类型的 head operator 专用的：one-input、two-input task、
 * source，iteration heads 和 iteration tails
 *
 * <p>The Task class deals with the setup of the streams read by the head operator, and the streams
 * produced by the operators at the ends of the operator chain. Note that the chain may fork and
 * thus have multiple ends.
 *
 * StreamTask 通过读取 head operator 建立 stream
 * 并且 stream 由操作链的末端操作符产生
 * 需要注意的是，chain 可能 fork 因此出现多个末端
 *
 * <p>The life cycle of the task is set up as follows:
 * task 生命周期的建立如下所示：
 * <pre>{@code
 *  -- setInitialState -> provides state of all operators in the chain
 *  -- 设置操作链中所有操作符的初始状态
 *  -- invoke()
 *        |
 *        +----> Create basic utils (config, etc) and load the chain of operators
 *        +----> operators.setup()
 *        +----> task specific init()
 *        +----> initialize-operator-states()
 *        +----> open-operators()
 *        +----> run()
 *        +----> close-operators()
 *        +----> dispose-operators()
 *        +----> common cleanup
 *        +----> task specific cleanup()
 * }</pre>
 *
 * <p>The {@code StreamTask} has a lock object called {@code lock}. All calls to methods on a
 * {@code StreamOperator} must be synchronized on this lock object to ensure that no methods
 * are called concurrently.
 *
 * StreamTask 有一个锁对象叫做 lock。StreamOperator 必须竞争这个锁来保证没有方法被并发调用
 *
 * @param <OUT>
 * @param <OP>
 */
@Internal
public abstract class StreamTask<OUT, OP extends StreamOperator<OUT>>
		extends AbstractInvokable
		implements AsyncExceptionHandler {

	/** The thread group that holds all trigger timer threads. */
	// 包含所有触发器计时器线程的线程组
	public static final ThreadGroup TRIGGER_THREAD_GROUP = new ThreadGroup("Triggers");

	/** The logger used by the StreamTask and its subclasses. */
	// StreamTask 和其子类使用的 logger
	private static final Logger LOG = LoggerFactory.getLogger(StreamTask.class);

	// ------------------------------------------------------------------------

	/**
	 * All interaction with the {@code StreamOperator} must be synchronized on this lock object to
	 * ensure that we don't have concurrent method calls that void consistent checkpoints.
	 */
	// 必须在此锁定对象上同步与StreamOperator的所有交互，以确保我们没有使一致检查点无效的并发方法调用
	private final Object lock = new Object();

	/** the head operator that consumes the input streams of this task. */
	// 消费 task 输入流的头部操作符
	protected OP headOperator;

	/** The chain of operators executed by this task. */
	// task 执行的操作符链
	protected OperatorChain<OUT, OP> operatorChain;

	/** The configuration of this streaming task. */
	// 流任务的配置
	protected final StreamConfig configuration;

	/** Our state backend. We use this to create checkpoint streams and a keyed state backend. */
	// 状态 backend，使用它来创建检查点流以及一个 keyed state backend
	protected StateBackend stateBackend;

	/** The external storage where checkpoint data is persisted. */
	// 持久存储检查点数据的外部存储
	private CheckpointStorageWorkerView checkpointStorage;

	/**
	 * The internal {@link ProcessingTimeService} used to define the current
	 * processing time (default = {@code System.currentTimeMillis()}) and
	 * register timers for tasks to be executed in the future.
	 */
	/**
	 * 内部 ProcessingTimeService 用于定义当前处理时间(default = System.currentTimeMillis())
	 * 并注册将来要执行的任务的计时器
	 */
	protected ProcessingTimeService timerService;

	/** The map of user-defined accumulators of this task. */
	// 此任务的用户定义累加器的映射
	private final Map<String, Accumulator<?, ?>> accumulatorMap;

	/** The currently active background materialization threads. */
	// 用于需要关闭的实例注册，然后在服务挂掉的时候，统一 close，释放资源
	private final CloseableRegistry cancelables = new CloseableRegistry();

	/**
	 * Flag to mark the task "in operation", in which case check needs to be initialized to true,
	 * so that early cancel() before invoke() behaves correctly.
	 */
	// 标记任务“正在运行”的标志，在这种情况下，检查需要初始化为 true
	// 以便在 invoke() 之前的早期 cancel() 行为正确
	private volatile boolean isRunning;

	/** Flag to mark this task as canceled. */
	// 标记 task 为取消的
	private volatile boolean canceled;

	/** Thread pool for async snapshot workers. */
	// 异步快照 workers 使用的线程池
	private ExecutorService asyncOperationsThreadPool;

	/** Handler for exceptions during checkpointing in the stream task. Used in synchronous part of the checkpoint. */
	// 在流任务中检查点期间处理异常，用于检查点的同步部分
	private CheckpointExceptionHandler synchronousCheckpointExceptionHandler;

	/** Wrapper for synchronousCheckpointExceptionHandler to deal with rethrown exceptions. Used in the async part. */
	// 用于 synchronCheckpointExceptionHandler 的包装器
	// 用于处理重新抛出的异常，用于异步部分
	private AsyncCheckpointExceptionHandler asynchronousCheckpointExceptionHandler;

	private final List<RecordWriter<SerializationDelegate<StreamRecord<OUT>>>> recordWriters;

	// ------------------------------------------------------------------------

	/**
	 * Constructor for initialization, possibly with initial state (recovery / savepoint / etc).
	 *
	 * @param env The task environment for this task.
	 */
	/**
	 * 用于初始化的构造函数，可能具有初始状态（恢复/保存点/等）
	 */
	protected StreamTask(Environment env) {
		this(env, null);
	}

	/**
	 * Constructor for initialization, possibly with initial state (recovery / savepoint / etc).
	 *
	 * <p>This constructor accepts a special {@link ProcessingTimeService}. By default (and if
	 * null is passes for the time provider) a {@link SystemProcessingTimeService DefaultTimerService}
	 * will be used.
	 *
	 * @param environment The task environment for this task.
	 * @param timeProvider Optionally, a specific time provider to use.
	 */
	/**
	 * 用于初始化的构造函数，可能具有初始状态（恢复/保存点/等）
	 * 
	 * 此构造函数接受特殊的 ProcessingTimeService
	 * 默认情况下（如果时间提供程序为null），将使用 SystemProcessingTimeService
	 */
	protected StreamTask(
			Environment environment,
			@Nullable ProcessingTimeService timeProvider) {

		super(environment);

		this.timerService = timeProvider;
		this.configuration = new StreamConfig(getTaskConfiguration());
		this.accumulatorMap = getEnvironment().getAccumulatorRegistry().getUserMap();
		this.recordWriters = createRecordWriters(configuration, environment);
	}

	// ------------------------------------------------------------------------
	//  Life cycle methods for specific implementations
	// ------------------------------------------------------------------------

	protected abstract void init() throws Exception;

	protected abstract void run() throws Exception;

	protected abstract void cleanup() throws Exception;

	protected abstract void cancelTask() throws Exception;

	// ------------------------------------------------------------------------
	//  Core work methods of the Stream Task
	// ------------------------------------------------------------------------

	public StreamTaskStateInitializer createStreamTaskStateInitializer() {
		return new StreamTaskStateInitializerImpl(
			getEnvironment(),
			stateBackend,
			timerService);
	}

	@Override
	public final void invoke() throws Exception {

		boolean disposed = false;
		try {
			// -------- Initialize ---------
			LOG.debug("Initializing {}.", getName());

			// 异步操作线程池
			asyncOperationsThreadPool = Executors.newCachedThreadPool();

			// 生成检查点异常处理工厂的实例
			CheckpointExceptionHandlerFactory cpExceptionHandlerFactory = createCheckpointExceptionHandlerFactory();

			// 获取同步检查点异常处理方式
			synchronousCheckpointExceptionHandler = cpExceptionHandlerFactory.createCheckpointExceptionHandler(
				getExecutionConfig().isFailTaskOnCheckpointError(),
				getEnvironment());
			
			// 异步检查点异常处理方式
			asynchronousCheckpointExceptionHandler = new AsyncCheckpointExceptionHandler(this);

			stateBackend = createStateBackend();
			checkpointStorage = stateBackend.createCheckpointStorage(getEnvironment().getJobID());

			// if the clock is not already set, then assign a default TimeServiceProvider
			// 如果 clock 没有设置，使用 SystemProcessingTimeService
			if (timerService == null) {
				ThreadFactory timerThreadFactory = new DispatcherThreadFactory(TRIGGER_THREAD_GROUP,
					"Time Trigger for " + getName(), getUserCodeClassLoader());

				timerService = new SystemProcessingTimeService(this, getCheckpointLock(), timerThreadFactory);
			}

			operatorChain = new OperatorChain<>(this, recordWriters);
			headOperator = operatorChain.getHeadOperator();

			// task specific initialization
			init();

			// save the work of reloading state, etc, if the task is already canceled
			// 如果任务已被取消，则保存重新加载状态等的工作
			if (canceled) {
				throw new CancelTaskException();
			}

			// -------- Invoke --------
			LOG.debug("Invoking {}", getName());

			// we need to make sure that any triggers scheduled in open() cannot be
			// executed before all operators are opened
			// 我们需要确保在打开所有运算符之前无法执行 open() 中调度的任何触发器
			synchronized (lock) {

				// both the following operations are protected by the lock
				// so that we avoid race conditions in the case that initializeState()
				// registers a timer, that fires before the open() is called.
				// 以下操作都受锁保护，以便在 initializeState() 注册一个计时器时避免竞争条件
				// 该计时器在调用 open() 前触发

				initializeState();
				openAllOperators();
			}

			// final check to exit early before starting to run
			if (canceled) {
				throw new CancelTaskException();
			}

			// let the task do its work
			isRunning = true;
			run();  // SourceStreamTask 重写了这个方法，会调用头部操作符的 run 方法，启动服务

			// if this left the run() method cleanly despite the fact that this was canceled,
			// make sure the "clean shutdown" is not attempted
			if (canceled) {
				throw new CancelTaskException();
			}

			LOG.debug("Finished task {}", getName());

			// make sure no further checkpoint and notification actions happen.
			// we make sure that no other thread is currently in the locked scope before
			// we close the operators by trying to acquire the checkpoint scope lock
			// we also need to make sure that no triggers fire concurrently with the close logic
			// at the same time, this makes sure that during any "regular" exit where still
			synchronized (lock) {
				// this is part of the main logic, so if this fails, the task is considered failed
				closeAllOperators();

				// make sure no new timers can come
				timerService.quiesce();

				// only set the StreamTask to not running after all operators have been closed!
				// See FLINK-7430
				isRunning = false;
			}

			// make sure all timers finish
			timerService.awaitPendingAfterQuiesce();

			LOG.debug("Closed operators for task {}", getName());

			// make sure all buffered data is flushed
			// 确保所有缓存的数据 flush
			operatorChain.flushOutputs();

			// make an attempt to dispose the operators such that failures in the dispose call
			// still let the computation fail
			tryDisposeAllOperators();
			disposed = true;
		}
		finally {
			// clean up everything we initialized
			// clean up 我们初始化的所有东西
			isRunning = false;

			// Now that we are outside the user code, we do not want to be interrupted further
			// upon cancellation. The shutdown logic below needs to make sure it does not issue calls
			// that block and stall shutdown.
			// Additionally, the cancellation watch dog will issue a hard-cancel (kill the TaskManager
			// process) as a backup in case some shutdown procedure blocks outside our control.
			setShouldInterruptOnCancel(false);

			// clear any previously issued interrupt for a more graceful shutdown
			// 清除任何先前发出的中断以获得更优雅的关闭
			Thread.interrupted();

			// stop all timers and threads
			// 停止所有的定时器
			tryShutdownTimerService();

			// stop all asynchronous checkpoint threads
			// 停止所有的异步检查点线程
			try {
				cancelables.close();
				shutdownAsyncThreads();
			}
			catch (Throwable t) {
				// catch and log the exception to not replace the original exception
				LOG.error("Could not shut down async checkpoint threads", t);
			}

			// we must! perform this cleanup
			try {
				cleanup();
			}
			catch (Throwable t) {
				// catch and log the exception to not replace the original exception
				LOG.error("Error during cleanup of stream task", t);
			}

			// if the operators were not disposed before, do a hard dispose
			if (!disposed) {
				disposeAllOperators();
			}

			// release the output resources. this method should never fail.
			// 释放 output resources，释放 recordWriter
			if (operatorChain != null) {
				// beware: without synchronization, #performCheckpoint() may run in
				//         parallel and this call is not thread-safe
				// 如果不使用 synchronized，performCheckpoint 方法可能和本方法一起调用
				// 不是线程安全的了
				synchronized (lock) {
					operatorChain.releaseOutputs();
				}
			}
		}
	}

	@Override
	public final void cancel() throws Exception {
		isRunning = false;
		canceled = true;

		// the "cancel task" call must come first, but the cancelables must be
		// closed no matter what
		try {
			cancelTask();
		}
		finally {
			cancelables.close();
		}
	}

	public final boolean isRunning() {
		return isRunning;
	}

	public final boolean isCanceled() {
		return canceled;
	}

	/**
	 * Execute {@link StreamOperator#open()} of each operator in the chain of this
	 * {@link StreamTask}. Opening happens from <b>tail to head</b> operator in the chain, contrary
	 * to {@link StreamOperator#close()} which happens <b>head to tail</b>
	 * (see {@link #closeAllOperators()}.
	 */
	/**
	 * 执行操作链中所有操作符的 open 方法
	 * 从尾到头 open
	 */
	private void openAllOperators() throws Exception {
		for (StreamOperator<?> operator : operatorChain.getAllOperators()) {
			if (operator != null) {
				operator.open();
			}
		}
	}

	/**
	 * Execute {@link StreamOperator#close()} of each operator in the chain of this
	 * {@link StreamTask}. Closing happens from <b>head to tail</b> operator in the chain,
	 * contrary to {@link StreamOperator#open()} which happens <b>tail to head</b>
	 * (see {@link #openAllOperators()}.
	 */
	/**
	 * 执行操作链中所有操作符的 close 方法
	 * 从头到尾 close
	 */
	private void closeAllOperators() throws Exception {
		// We need to close them first to last, since upstream operators in the chain might emit
		// elements in their close methods.
		// 我们需要从头到尾关闭，因为链中的上游操作符可能在 close 方法中 emit elements
		StreamOperator<?>[] allOperators = operatorChain.getAllOperators();
		for (int i = allOperators.length - 1; i >= 0; i--) {
			StreamOperator<?> operator = allOperators[i];
			if (operator != null) {
				operator.close();
			}
		}
	}

	/**
	 * Execute {@link StreamOperator#dispose()} of each operator in the chain of this
	 * {@link StreamTask}. Disposing happens from <b>tail to head</b> operator in the chain.
	 */
	/**
	 * 执行操作链中所有操作符的 dispose 方法
	 */
	private void tryDisposeAllOperators() throws Exception {
		for (StreamOperator<?> operator : operatorChain.getAllOperators()) {
			if (operator != null) {
				operator.dispose();
			}
		}
	}

	// 关闭异步处理线程
	private void shutdownAsyncThreads() throws Exception {
		if (!asyncOperationsThreadPool.isShutdown()) {
			asyncOperationsThreadPool.shutdownNow();
		}
	}

	/**
	 * Execute @link StreamOperator#dispose()} of each operator in the chain of this
	 * {@link StreamTask}. Disposing happens from <b>tail to head</b> operator in the chain.
	 *
	 * <p>The difference with the {@link #tryDisposeAllOperators()} is that in case of an
	 * exception, this method catches it and logs the message.
	 */
	/**
	 * 执行操作链中所有操作符的 dispose 方法，本方法与 tryDisposeAllOperators 的不同在于
	 * 本方法会捕获 operator.dispose 方法抛出的异常
	 */
	private void disposeAllOperators() {
		if (operatorChain != null) {
			for (StreamOperator<?> operator : operatorChain.getAllOperators()) {
				try {
					if (operator != null) {
						operator.dispose();
					}
				}
				catch (Throwable t) {
					LOG.error("Error during disposal of stream operator.", t);
				}
			}
		}
	}

	/**
	 * The finalize method shuts down the timer. This is a fail-safe shutdown, in case the original
	 * shutdown method was never called.
	 *
	 * <p>This should not be relied upon! It will cause shutdown to happen much later than if manual
	 * shutdown is attempted, and cause threads to linger for longer than needed.
	 */
	/**
	 * finalize 方法关闭计时器。这是一个故障安全的关闭，以防从未调用原始关闭方法
	 * 
	 * 这不应该依赖！与尝试手动关闭相比，它将导致关闭发生的时间要晚得多，并导致线程停留的时间超过所需的时间
	 */
	@Override
	protected void finalize() throws Throwable {
		super.finalize();
		if (timerService != null) {
			if (!timerService.isTerminated()) {
				LOG.info("Timer service is shutting down.");
				timerService.shutdownService();
			}
		}

		cancelables.close();
	}

	boolean isSerializingTimestamps() {
		TimeCharacteristic tc = configuration.getTimeCharacteristic();
		return tc == TimeCharacteristic.EventTime | tc == TimeCharacteristic.IngestionTime;
	}

	// ------------------------------------------------------------------------
	//  Access to properties and utilities
	// ------------------------------------------------------------------------

	/**
	 * Gets the name of the task, in the form "taskname (2/5)".
	 * @return The name of the task.
	 */
	public String getName() {
		return getEnvironment().getTaskInfo().getTaskNameWithSubtasks();
	}

	/**
	 * Gets the lock object on which all operations that involve data and state mutation have to lock.
	 * @return The checkpoint lock object.
	 */
	public Object getCheckpointLock() {
		return lock;
	}

	public CheckpointStorageWorkerView getCheckpointStorage() {
		return checkpointStorage;
	}

	public StreamConfig getConfiguration() {
		return configuration;
	}

	public Map<String, Accumulator<?, ?>> getAccumulatorMap() {
		return accumulatorMap;
	}

	public StreamStatusMaintainer getStreamStatusMaintainer() {
		return operatorChain;
	}

	RecordWriterOutput<?>[] getStreamOutputs() {
		return operatorChain.getStreamOutputs();
	}

	// ------------------------------------------------------------------------
	//  Checkpoint and Restore
	// ------------------------------------------------------------------------

	// 触发检查点
	@Override
	public boolean triggerCheckpoint(CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions) throws Exception {
		try {
			// No alignment if we inject a checkpoint
			CheckpointMetrics checkpointMetrics = new CheckpointMetrics()
					.setBytesBufferedInAlignment(0L)
					.setAlignmentDurationNanos(0L);

			return performCheckpoint(checkpointMetaData, checkpointOptions, checkpointMetrics);
		}
		catch (Exception e) {
			// propagate exceptions only if the task is still in "running" state
			if (isRunning) {
				throw new Exception("Could not perform checkpoint " + checkpointMetaData.getCheckpointId() +
					" for operator " + getName() + '.', e);
			} else {
				LOG.debug("Could not perform checkpoint {} for operator {} while the " +
					"invokable was not in state running.", checkpointMetaData.getCheckpointId(), getName(), e);
				return false;
			}
		}
	}

	@Override
	public void triggerCheckpointOnBarrier(
			CheckpointMetaData checkpointMetaData,
			CheckpointOptions checkpointOptions,
			CheckpointMetrics checkpointMetrics) throws Exception {

		try {
			performCheckpoint(checkpointMetaData, checkpointOptions, checkpointMetrics);
		}
		catch (CancelTaskException e) {
			LOG.info("Operator {} was cancelled while performing checkpoint {}.",
					getName(), checkpointMetaData.getCheckpointId());
			throw e;
		}
		catch (Exception e) {
			throw new Exception("Could not perform checkpoint " + checkpointMetaData.getCheckpointId() + " for operator " +
				getName() + '.', e);
		}
	}

	// 停止检查点
	@Override
	public void abortCheckpointOnBarrier(long checkpointId, Throwable cause) throws Exception {
		LOG.debug("Aborting checkpoint via cancel-barrier {} for task {}", checkpointId, getName());

		// notify the coordinator that we decline this checkpoint
		// 通知协调员我们拒绝此检查点
		getEnvironment().declineCheckpoint(checkpointId, cause);

		// notify all downstream operators that they should not wait for a barrier from us
		// 通知所有下游操作符，他们不应该等我们的障碍
		synchronized (lock) {
			operatorChain.broadcastCheckpointCancelMarker(checkpointId);
		}
	}

	// 检查点操作
	private boolean performCheckpoint(
			CheckpointMetaData checkpointMetaData,
			CheckpointOptions checkpointOptions,
			CheckpointMetrics checkpointMetrics) throws Exception {

		LOG.debug("Starting checkpoint ({}) {} on task {}",
			checkpointMetaData.getCheckpointId(), checkpointOptions.getCheckpointType(), getName());

		synchronized (lock) {
			if (isRunning) {
				// we can do a checkpoint
				// 我们可以产生一个检查点
				// All of the following steps happen as an atomic step from the perspective of barriers and
				// records/watermarks/timers/callbacks.
				// We generally try to emit the checkpoint barrier as soon as possible to not affect downstream
				// checkpoint alignments
				// 从障碍和记录/水印/定时器/回调的角度来看，所有以下步骤都是作为原子步骤发生的

				// Step (1): Prepare the checkpoint, allow operators to do some pre-barrier work.
				//           The pre-barrier work should be nothing or minimal in the common case.
				// 步骤一：准备检查点，允许操作符做一些 pre-barrier 的工作
				operatorChain.prepareSnapshotPreBarrier(checkpointMetaData.getCheckpointId());

				// Step (2): Send the checkpoint barrier downstream
				// 步骤二：将 checkpoint barrier 传给下游
				operatorChain.broadcastCheckpointBarrier(
						checkpointMetaData.getCheckpointId(),
						checkpointMetaData.getTimestamp(),
						checkpointOptions);

				// Step (3): Take the state snapshot. This should be largely asynchronous, to not
				//           impact progress of the streaming topology
				// 步骤三：拍摄状态快照。这应该在很大程度上是异步的，不会影响流式拓扑的进度
				checkpointState(checkpointMetaData, checkpointOptions, checkpointMetrics);
				return true;
			}
			else {
				// we cannot perform our checkpoint - let the downstream operators know that they
				// should not wait for any input from this operator
				// 我们无法执行检查点 - 让下游操作符知道他们不应该等待来自此操作符的任何输入，以免影响流式拓扑的进度
				// we cannot broadcast the cancellation markers on the 'operator chain', because it may not
				// yet be created
				// 我们不能操作符链上广播取消标记，因为它可能尚未创建，不能调用 operatorChain.broadcastCheckpointCancelMarker() 方法
				final CancelCheckpointMarker message = new CancelCheckpointMarker(checkpointMetaData.getCheckpointId());
				Exception exception = null;

				for (RecordWriter<SerializationDelegate<StreamRecord<OUT>>> recordWriter : recordWriters) {
					try {
						recordWriter.broadcastEvent(message);
					} catch (Exception e) {
						exception = ExceptionUtils.firstOrSuppressed(
							new Exception("Could not send cancel checkpoint marker to downstream tasks.", e),
							exception);
					}
				}

				if (exception != null) {
					throw exception;
				}

				return false;
			}
		}
	}

	public ExecutorService getAsyncOperationsThreadPool() {
		return asyncOperationsThreadPool;
	}

	// 通知检查点完成
	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		synchronized (lock) {
			if (isRunning) {
				LOG.debug("Notification of complete checkpoint for task {}", getName());

				for (StreamOperator<?> operator : operatorChain.getAllOperators()) {
					if (operator != null) {
						operator.notifyCheckpointComplete(checkpointId);
					}
				}
			}
			else {
				LOG.debug("Ignoring notification of complete checkpoint for not-running task {}", getName());
			}
		}
	}

	// 尝试关闭时间定时器服务
	private void tryShutdownTimerService() {

		if (timerService != null && !timerService.isTerminated()) {

			try {
				final long timeoutMs = getEnvironment().getTaskManagerInfo().getConfiguration().
					getLong(TaskManagerOptions.TASK_CANCELLATION_TIMEOUT_TIMERS);

				if (!timerService.shutdownServiceUninterruptible(timeoutMs)) {
					LOG.warn("Timer service shutdown exceeded time limit of {} ms while waiting for pending " +
						"timers. Will continue with shutdown procedure.", timeoutMs);
				}
			} catch (Throwable t) {
				// catch and log the exception to not replace the original exception
				LOG.error("Could not shut down timer service", t);
			}
		}
	}

	// 检查点状态
	private void checkpointState(
			CheckpointMetaData checkpointMetaData,
			CheckpointOptions checkpointOptions,
			CheckpointMetrics checkpointMetrics) throws Exception {

		CheckpointStreamFactory storage = checkpointStorage.resolveCheckpointStorageLocation(
				checkpointMetaData.getCheckpointId(),
				checkpointOptions.getTargetLocation());

		CheckpointingOperation checkpointingOperation = new CheckpointingOperation(
			this,
			checkpointMetaData,
			checkpointOptions,
			storage,
			checkpointMetrics);

		checkpointingOperation.executeCheckpointing();
	}

	// 调用操作链中所有操作符的 initializeState 方法
	private void initializeState() throws Exception {

		StreamOperator<?>[] allOperators = operatorChain.getAllOperators();

		for (StreamOperator<?> operator : allOperators) {
			if (null != operator) {
				operator.initializeState();
			}
		}
	}

	// ------------------------------------------------------------------------
	//  State backend
	// ------------------------------------------------------------------------

	private StateBackend createStateBackend() throws Exception {
		final StateBackend fromApplication = configuration.getStateBackend(getUserCodeClassLoader());

		return StateBackendLoader.fromApplicationOrConfigOrDefault(
				fromApplication,
				getEnvironment().getTaskManagerInfo().getConfiguration(),
				getUserCodeClassLoader(),
				LOG);
	}

	// 创建检查点异常处理工厂实例
	protected CheckpointExceptionHandlerFactory createCheckpointExceptionHandlerFactory() {
		return new CheckpointExceptionHandlerFactory();
	}

	/**
	 * Returns the {@link ProcessingTimeService} responsible for telling the current
	 * processing time and registering timers.
	 */
	// 返回时间定时器服务
	public ProcessingTimeService getProcessingTimeService() {
		if (timerService == null) {
			throw new IllegalStateException("The timer service has not been initialized.");
		}
		return timerService;
	}

	/**
	 * Handles an exception thrown by another thread (e.g. a TriggerTask),
	 * other than the one executing the main task by failing the task entirely.
	 *
	 * <p>In more detail, it marks task execution failed for an external reason
	 * (a reason other than the task code itself throwing an exception). If the task
	 * is already in a terminal state (such as FINISHED, CANCELED, FAILED), or if the
	 * task is already canceling this does nothing. Otherwise it sets the state to
	 * FAILED, and, if the invokable code is running, starts an asynchronous thread
	 * that aborts that code.
	 *
	 * <p>This method never blocks.
	 */
	@Override
	public void handleAsyncException(String message, Throwable exception) {
		if (isRunning) {
			// only fail if the task is still running
			getEnvironment().failExternally(exception);
		}
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	@Override
	public String toString() {
		return getName();
	}

	// ------------------------------------------------------------------------

	/**
	 * This runnable executes the asynchronous parts of all involved backend snapshots for the subtask.
	 */
	/**
	 * 此 runnable 执行子任务的所有相关后端快照的异步部分
	 */
	@VisibleForTesting
	protected static final class AsyncCheckpointRunnable implements Runnable, Closeable {

		private final StreamTask<?, ?> owner;

		private final Map<OperatorID, OperatorSnapshotFutures> operatorSnapshotsInProgress;  // 所有 operator 的快照 map

		private final CheckpointMetaData checkpointMetaData;
		private final CheckpointMetrics checkpointMetrics;

		private final long asyncStartNanos;  // 异步开始的时间

		private final AtomicReference<CheckpointingOperation.AsyncCheckpointState> asyncCheckpointState = new AtomicReference<>(
			CheckpointingOperation.AsyncCheckpointState.RUNNING);  // 异步检查点状态

		AsyncCheckpointRunnable(
			StreamTask<?, ?> owner,
			Map<OperatorID, OperatorSnapshotFutures> operatorSnapshotsInProgress,
			CheckpointMetaData checkpointMetaData,
			CheckpointMetrics checkpointMetrics,
			long asyncStartNanos) {

			this.owner = Preconditions.checkNotNull(owner);
			this.operatorSnapshotsInProgress = Preconditions.checkNotNull(operatorSnapshotsInProgress);
			this.checkpointMetaData = Preconditions.checkNotNull(checkpointMetaData);
			this.checkpointMetrics = Preconditions.checkNotNull(checkpointMetrics);
			this.asyncStartNanos = asyncStartNanos;
		}

		@Override
		public void run() {
			FileSystemSafetyNet.initializeSafetyNetForThread();
			try {

				TaskStateSnapshot jobManagerTaskOperatorSubtaskStates =
					new TaskStateSnapshot(operatorSnapshotsInProgress.size());

				TaskStateSnapshot localTaskOperatorSubtaskStates =
					new TaskStateSnapshot(operatorSnapshotsInProgress.size());

				for (Map.Entry<OperatorID, OperatorSnapshotFutures> entry : operatorSnapshotsInProgress.entrySet()) {

					OperatorID operatorID = entry.getKey();
					OperatorSnapshotFutures snapshotInProgress = entry.getValue();

					// finalize the async part of all by executing all snapshot runnables
					// 通过执行所有快照可运行来完成所有异步部分
					OperatorSnapshotFinalizer finalizedSnapshots =
						new OperatorSnapshotFinalizer(snapshotInProgress);

					jobManagerTaskOperatorSubtaskStates.putSubtaskStateByOperatorID(
						operatorID,
						finalizedSnapshots.getJobManagerOwnedState());

					localTaskOperatorSubtaskStates.putSubtaskStateByOperatorID(
						operatorID,
						finalizedSnapshots.getTaskLocalState());
				}

				final long asyncEndNanos = System.nanoTime();
				final long asyncDurationMillis = (asyncEndNanos - asyncStartNanos) / 1_000_000L;

				checkpointMetrics.setAsyncDurationMillis(asyncDurationMillis);
				
				// 全部执行完毕，更新状态
				if (asyncCheckpointState.compareAndSet(CheckpointingOperation.AsyncCheckpointState.RUNNING,
					CheckpointingOperation.AsyncCheckpointState.COMPLETED)) {

					reportCompletedSnapshotStates(
						jobManagerTaskOperatorSubtaskStates,
						localTaskOperatorSubtaskStates,
						asyncDurationMillis);

				} else {
					LOG.debug("{} - asynchronous part of checkpoint {} could not be completed because it was closed before.",
						owner.getName(),
						checkpointMetaData.getCheckpointId());
				}
			} catch (Exception e) {
				handleExecutionException(e);
			} finally {
				owner.cancelables.unregisterCloseable(this);
				FileSystemSafetyNet.closeSafetyNetAndGuardedResourcesForThread();
			}
		}

		// 报告快照生成完毕
		private void reportCompletedSnapshotStates(
			TaskStateSnapshot acknowledgedTaskStateSnapshot,
			TaskStateSnapshot localTaskStateSnapshot,
			long asyncDurationMillis) {

			// 获取当前 task 执行的 taskManager
			TaskStateManager taskStateManager = owner.getEnvironment().getTaskStateManager();

			boolean hasAckState = acknowledgedTaskStateSnapshot.hasState();
			boolean hasLocalState = localTaskStateSnapshot.hasState();

			Preconditions.checkState(hasAckState || !hasLocalState,
				"Found cached state but no corresponding primary state is reported to the job " +
					"manager. This indicates a problem.");

			// we signal stateless tasks by reporting null, so that there are no attempts to assign empty state
			// to stateless tasks on restore. This enables simple job modifications that only concern
			// stateless without the need to assign them uids to match their (always empty) states.
			// 我们通过报告 null 来表示无状态任务，因此在还原时没有尝试将空状态分配给无状态任务
			// 这使得简单的作业修改只涉及无状态，而无需在失败任务的时候为其分配 uid 来匹配它们总是空的状态
			taskStateManager.reportTaskStateSnapshots(
				checkpointMetaData,
				checkpointMetrics,
				hasAckState ? acknowledgedTaskStateSnapshot : null,
				hasLocalState ? localTaskStateSnapshot : null);

			LOG.debug("{} - finished asynchronous part of checkpoint {}. Asynchronous duration: {} ms",
				owner.getName(), checkpointMetaData.getCheckpointId(), asyncDurationMillis);

			LOG.trace("{} - reported the following states in snapshot for checkpoint {}: {}.",
				owner.getName(), checkpointMetaData.getCheckpointId(), acknowledgedTaskStateSnapshot);
		}

		// 处理执行异常
		private void handleExecutionException(Exception e) {

			boolean didCleanup = false;
			CheckpointingOperation.AsyncCheckpointState currentState = asyncCheckpointState.get();

			while (CheckpointingOperation.AsyncCheckpointState.DISCARDED != currentState) {

				if (asyncCheckpointState.compareAndSet(
					currentState,
					CheckpointingOperation.AsyncCheckpointState.DISCARDED)) {

					didCleanup = true;

					try {
						cleanup();
					} catch (Exception cleanupException) {
						e.addSuppressed(cleanupException);
					}

					Exception checkpointException = new Exception(
						"Could not materialize checkpoint " + checkpointMetaData.getCheckpointId() + " for operator " +
							owner.getName() + '.',
						e);

					// We only report the exception for the original cause of fail and cleanup.
					// Otherwise this followup exception could race the original exception in failing the task.
					// 我们只报告失败和清理的原始异常
					// 否则，此后续异常可能会在失败任务时使原始异常竞争
					owner.asynchronousCheckpointExceptionHandler.tryHandleCheckpointException(
						checkpointMetaData,
						checkpointException);

					currentState = CheckpointingOperation.AsyncCheckpointState.DISCARDED;
				} else {
					currentState = asyncCheckpointState.get();
				}
			}

			if (!didCleanup) {
				LOG.trace("Caught followup exception from a failed checkpoint thread. This can be ignored.", e);
			}
		}

		@Override
		public void close() {
			if (asyncCheckpointState.compareAndSet(
				CheckpointingOperation.AsyncCheckpointState.RUNNING,
				CheckpointingOperation.AsyncCheckpointState.DISCARDED)) {

				try {
					cleanup();
				} catch (Exception cleanupException) {
					LOG.warn("Could not properly clean up the async checkpoint runnable.", cleanupException);
				}
			} else {
				logFailedCleanupAttempt();
			}
		}

		private void cleanup() throws Exception {
			LOG.debug(
				"Cleanup AsyncCheckpointRunnable for checkpoint {} of {}.",
				checkpointMetaData.getCheckpointId(),
				owner.getName());

			Exception exception = null;

			// clean up ongoing operator snapshot results and non partitioned state handles
			for (OperatorSnapshotFutures operatorSnapshotResult : operatorSnapshotsInProgress.values()) {
				if (operatorSnapshotResult != null) {
					try {
						operatorSnapshotResult.cancel();
					} catch (Exception cancelException) {
						exception = ExceptionUtils.firstOrSuppressed(cancelException, exception);
					}
				}
			}

			if (null != exception) {
				throw exception;
			}
		}

		private void logFailedCleanupAttempt() {
			LOG.debug("{} - asynchronous checkpointing operation for checkpoint {} has " +
					"already been completed. Thus, the state handles are not cleaned up.",
				owner.getName(),
				checkpointMetaData.getCheckpointId());
		}
	}

	public CloseableRegistry getCancelables() {
		return cancelables;
	}

	// ------------------------------------------------------------------------
	// 检查点操作
	private static final class CheckpointingOperation {

		private final StreamTask<?, ?> owner;

		private final CheckpointMetaData checkpointMetaData;
		private final CheckpointOptions checkpointOptions;
		private final CheckpointMetrics checkpointMetrics;
		private final CheckpointStreamFactory storageLocation;

		private final StreamOperator<?>[] allOperators;

		private long startSyncPartNano;
		private long startAsyncPartNano;

		// ------------------------

		private final Map<OperatorID, OperatorSnapshotFutures> operatorSnapshotsInProgress;

		public CheckpointingOperation(
				StreamTask<?, ?> owner,
				CheckpointMetaData checkpointMetaData,
				CheckpointOptions checkpointOptions,
				CheckpointStreamFactory checkpointStorageLocation,
				CheckpointMetrics checkpointMetrics) {

			this.owner = Preconditions.checkNotNull(owner);
			this.checkpointMetaData = Preconditions.checkNotNull(checkpointMetaData);
			this.checkpointOptions = Preconditions.checkNotNull(checkpointOptions);
			this.checkpointMetrics = Preconditions.checkNotNull(checkpointMetrics);
			this.storageLocation = Preconditions.checkNotNull(checkpointStorageLocation);
			this.allOperators = owner.operatorChain.getAllOperators();
			this.operatorSnapshotsInProgress = new HashMap<>(allOperators.length);
		}

		public void executeCheckpointing() throws Exception {
			startSyncPartNano = System.nanoTime();

			try {
				// 执行每个操作符的 snapshotState 方法
				for (StreamOperator<?> op : allOperators) {
					checkpointStreamOperator(op);
				}

				if (LOG.isDebugEnabled()) {
					LOG.debug("Finished synchronous checkpoints for checkpoint {} on task {}",
						checkpointMetaData.getCheckpointId(), owner.getName());
				}

				startAsyncPartNano = System.nanoTime();

				checkpointMetrics.setSyncDurationMillis((startAsyncPartNano - startSyncPartNano) / 1_000_000);

				// we are transferring ownership over snapshotInProgressList for cleanup to the thread, active on submit
				// 我们正在通过 snapshotInProgressList 将所有权转移到线程，在提交时激活
				AsyncCheckpointRunnable asyncCheckpointRunnable = new AsyncCheckpointRunnable(
					owner,
					operatorSnapshotsInProgress,
					checkpointMetaData,
					checkpointMetrics,
					startAsyncPartNano);

				owner.cancelables.registerCloseable(asyncCheckpointRunnable);  // 注册 close
				owner.asyncOperationsThreadPool.submit(asyncCheckpointRunnable);  // 提交到线程池

				if (LOG.isDebugEnabled()) {
					LOG.debug("{} - finished synchronous part of checkpoint {}. " +
							"Alignment duration: {} ms, snapshot duration {} ms",
						owner.getName(), checkpointMetaData.getCheckpointId(),
						checkpointMetrics.getAlignmentDurationNanos() / 1_000_000,
						checkpointMetrics.getSyncDurationMillis());
				}
			} catch (Exception ex) {
				// Cleanup to release resources
				for (OperatorSnapshotFutures operatorSnapshotResult : operatorSnapshotsInProgress.values()) {
					if (null != operatorSnapshotResult) {
						try {
							operatorSnapshotResult.cancel();
						} catch (Exception e) {
							LOG.warn("Could not properly cancel an operator snapshot result.", e);
						}
					}
				}

				if (LOG.isDebugEnabled()) {
					LOG.debug("{} - did NOT finish synchronous part of checkpoint {}. " +
							"Alignment duration: {} ms, snapshot duration {} ms",
						owner.getName(), checkpointMetaData.getCheckpointId(),
						checkpointMetrics.getAlignmentDurationNanos() / 1_000_000,
						checkpointMetrics.getSyncDurationMillis());
				}

				owner.synchronousCheckpointExceptionHandler.tryHandleCheckpointException(checkpointMetaData, ex);
			}
		}

		@SuppressWarnings("deprecation")
		// 执行每个操作符的 snapshotState 方法
		private void checkpointStreamOperator(StreamOperator<?> op) throws Exception {
			if (null != op) {

				OperatorSnapshotFutures snapshotInProgress = op.snapshotState(
						checkpointMetaData.getCheckpointId(),
						checkpointMetaData.getTimestamp(),
						checkpointOptions,
						storageLocation);
				operatorSnapshotsInProgress.put(op.getOperatorID(), snapshotInProgress);
			}
		}

		// 异步检查点状态
		private enum AsyncCheckpointState {
			RUNNING,
			DISCARDED,
			COMPLETED
		}
	}

	/**
	 * Wrapper for synchronous {@link CheckpointExceptionHandler}. This implementation catches unhandled, rethrown
	 * exceptions and reports them through {@link #handleAsyncException(String, Throwable)}. As this implementation
	 * always handles the exception in some way, it never rethrows.
	 */
	/**
	 * 同步 CheckpointExceptionHandler 的包裹
	 * 本类捕获未处理的，重新抛出的异常，并且通过 handleAsyncException 方法来报告
	 * 由于此实现始终以某种方式处理异常，因此它永远不会重新抛出
	 */
	static final class AsyncCheckpointExceptionHandler implements CheckpointExceptionHandler {

		/** Owning stream task to which we report async exceptions. */
		final StreamTask<?, ?> owner;

		/** Synchronous exception handler to which we delegate. */
		final CheckpointExceptionHandler synchronousCheckpointExceptionHandler;

		AsyncCheckpointExceptionHandler(StreamTask<?, ?> owner) {
			this.owner = Preconditions.checkNotNull(owner);
			this.synchronousCheckpointExceptionHandler =
				Preconditions.checkNotNull(owner.synchronousCheckpointExceptionHandler);
		}

		@Override
		public void tryHandleCheckpointException(CheckpointMetaData checkpointMetaData, Exception exception) {
			try {
				synchronousCheckpointExceptionHandler.tryHandleCheckpointException(checkpointMetaData, exception);
			} catch (Exception unhandled) {
				AsynchronousException asyncException = new AsynchronousException(unhandled);
				owner.handleAsyncException("Failure in asynchronous checkpoint materialization", asyncException);
			}
		}
	}

	@VisibleForTesting
	// 构造函数初始化的时候调用
	public static <OUT> List<RecordWriter<SerializationDelegate<StreamRecord<OUT>>>> createRecordWriters(
			StreamConfig configuration,
			Environment environment) {
		List<RecordWriter<SerializationDelegate<StreamRecord<OUT>>>> recordWriters = new ArrayList<>();
		// 以下两个都是生成 JobGraph 的时候生成并写入 Config 的
		List<StreamEdge> outEdgesInOrder = configuration.getOutEdgesInOrder(environment.getUserClassLoader());
		Map<Integer, StreamConfig> chainedConfigs = configuration.getTransitiveChainedTaskConfigsWithSelf(environment.getUserClassLoader());

		for (int i = 0; i < outEdgesInOrder.size(); i++) {
			StreamEdge edge = outEdgesInOrder.get(i);
			recordWriters.add(
				createRecordWriter(
					edge,
					i,
					environment,
					environment.getTaskInfo().getTaskName(),
					chainedConfigs.get(edge.getSourceId()).getBufferTimeout()));
		}
		return recordWriters;
	}

	private static <OUT> RecordWriter<SerializationDelegate<StreamRecord<OUT>>> createRecordWriter(
			StreamEdge edge,
			int outputIndex,
			Environment environment,
			String taskName,
			long bufferTimeout) {
		@SuppressWarnings("unchecked")
		// 边上的 partitioner，没有显示设置的话，会使用 ForwardPartitioner 或 RebalancePartitioner
		StreamPartitioner<OUT> outputPartitioner = (StreamPartitioner<OUT>) edge.getPartitioner();

		LOG.debug("Using partitioner {} for output {} of task {}", outputPartitioner, outputIndex, taskName);

		ResultPartitionWriter bufferWriter = environment.getWriter(outputIndex);

		// we initialize the partitioner here with the number of key groups (aka max. parallelism)
		if (outputPartitioner instanceof ConfigurableStreamPartitioner) {
			int numKeyGroups = bufferWriter.getNumTargetKeyGroups();
			if (0 < numKeyGroups) {
				((ConfigurableStreamPartitioner) outputPartitioner).configure(numKeyGroups);
			}
		}

		RecordWriter<SerializationDelegate<StreamRecord<OUT>>> output =
			RecordWriter.createRecordWriter(bufferWriter, outputPartitioner, bufferTimeout, taskName);
		output.setMetricGroup(environment.getMetricGroup().getIOMetricGroup());
		return output;
	}
}
