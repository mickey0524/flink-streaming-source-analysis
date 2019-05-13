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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskManagerJobMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.DefaultKeyedStateStore;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupStatePartitionStreamProvider;
import org.apache.flink.runtime.state.KeyGroupsList;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.KeyedStateCheckpointOutputStream;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateInitializationContextImpl;
import org.apache.flink.runtime.state.StatePartitionStreamProvider;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.runtime.state.StateSnapshotContextSynchronousImpl;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.util.LatencyStats;
import org.apache.flink.util.CloseableIterable;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.Serializable;
import java.util.Locale;

/**
 * Base class for all stream operators. Operators that contain a user function should extend the class
 * {@link AbstractUdfStreamOperator} instead (which is a specialized subclass of this class).
 *
 * <p>For concrete implementations, one of the following two interfaces must also be implemented, to
 * mark the operator as unary or binary:
 * {@link OneInputStreamOperator} or {@link TwoInputStreamOperator}.
 *
 * <p>Methods of {@code StreamOperator} are guaranteed not to be called concurrently. Also, if using
 * the timer service, timer callbacks are also guaranteed not to be called concurrently with
 * methods on {@code StreamOperator}.
 *
 * @param <OUT> The output type of the operator
 */
/**
 * 所有 operator 的基类
 * 具体使用的话，需要实现 OneInputStreamOperator 接口或者 TwoInputStreamOperator 接口
 * StreamOperator 的方法需要保证不被并发调用。同理，如果使用了时间服务，定时器回调
 * 也需要保证不并发调用 StreamOperator 中的方法
 */
@PublicEvolving
public abstract class AbstractStreamOperator<OUT>
		implements StreamOperator<OUT>, Serializable {

	private static final long serialVersionUID = 1L;

	/** The logger used by the operator class and its subclasses. */
	protected static final Logger LOG = LoggerFactory.getLogger(AbstractStreamOperator.class);

	// ----------- configuration properties -------------

	// A sane default for most operators
	// 默认的链式操作策略，允许后续链式连接，不尝试链式连接前一个操作符
	protected ChainingStrategy chainingStrategy = ChainingStrategy.HEAD;

	// ---------------- runtime fields ------------------

	/** The task that contains this operator (and other operators in the same chain). */
	// StreamTask 包括了这个操作符（以及 chain 中的其他操作符）
	private transient StreamTask<?, ?> container;

	// 流配置文件，JobGraphGenerator 会将一个 chain 压缩为一个 JobVertex，头部操作符的 config 包括了链中后续操作符的 StreamConfig
	protected transient StreamConfig config;

	// operator 通过 output 来 emit element -> output.collect()
	// output 的生成位于 operatorChain.java 文件中
	// 链式内部的 output 用于调用 operator 的 processElement 方法
	// chain 连接到外部的 output 是一个 RecordWriter
	protected transient Output<StreamRecord<OUT>> output;

	/** The runtime context for UDFs. */
	// 用户定义函数的运行时上下文
	private transient StreamingRuntimeContext runtimeContext;

	// ---------------- key/value state ------------------

	/**
	 * {@code KeySelector} for extracting a key from an element being processed. This is used to
	 * scope keyed state to a key. This is null if the operator is not a keyed operator.
	 *
	 * <p>This is for elements from the first input.
	 */
	/**
	 * KeySelector 从正在被处理的 element 中提取出一个 key，用于 first input 的元素
	 * 当操作符不是 keyed 操作符的时候为空
	 */
	private transient KeySelector<?, ?> stateKeySelector1;

	/**
	 * {@code KeySelector} for extracting a key from an element being processed. This is used to
	 * scope keyed state to a key. This is null if the operator is not a keyed operator.
	 *
	 * <p>This is for elements from the second input.
	 */
	/**
	 * KeySelector 从正在被处理的 element 中提取出一个 key，用于 second input 的元素
	 * 当操作符不为 keyed 操作符的时候为空
	 */
	private transient KeySelector<?, ?> stateKeySelector2;

	/** Backend for keyed state. This might be empty if we're not on a keyed stream. */
	// keyed state 的 backend，当不是一个 keyed stream 的时候为空
	private transient AbstractKeyedStateBackend<?> keyedStateBackend;

	/** Keyed state store view on the keyed backend. */
	// keyedStateBackend 中 keyed Stream 的存储快照
	private transient DefaultKeyedStateStore keyedStateStore;

	// ---------------- operator state ------------------

	/** Operator state backend / store. */
	// 操作符状态 backend/store
	private transient OperatorStateBackend operatorStateBackend;

	// --------------- Metrics ---------------------------

	/** Metric group for the operator. */
	protected transient OperatorMetricGroup metrics;

	// 延迟状态
	protected transient LatencyStats latencyStats;

	// ---------------- time handler ------------------

	// 内部时间服务管理器
	protected transient InternalTimeServiceManager<?> timeServiceManager;

	// ---------------- two-input operator watermarks ------------------

	// We keep track of watermarks from both inputs, the combined input is the minimum
	// Once the minimum advances we emit a new watermark for downstream operators
	// 我们从所有输入跟踪 watermark，组合输入是其中 ts 较大的数值
	// 一旦最小值更新（新的 watermark ts 大于现在），我们向下游的所有操作符发送一个新的 watermark
	private long combinedWatermark = Long.MIN_VALUE;
	private long input1Watermark = Long.MIN_VALUE;
	private long input2Watermark = Long.MIN_VALUE;

	// ------------------------------------------------------------------------
	//  Life Cycle
	// ------------------------------------------------------------------------
	// 以下是生命周期方法
	@Override
	// setup 方法添加操作符对 context 和 output 的访问
	public void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<OUT>> output) {
		final Environment environment = containingTask.getEnvironment();  // 从 StreamTask 中获取当前的 runtime 环境
		this.container = containingTask; 
		this.config = config;
		try {
			OperatorMetricGroup operatorMetricGroup = environment.getMetricGroup().getOrAddOperator(config.getOperatorID(), config.getOperatorName());
			this.output = new CountingOutput(output, operatorMetricGroup.getIOMetricGroup().getNumRecordsOutCounter());
			if (config.isChainStart()) {
				operatorMetricGroup.getIOMetricGroup().reuseInputMetricsForTask();
			}
			if (config.isChainEnd()) {
				operatorMetricGroup.getIOMetricGroup().reuseOutputMetricsForTask();
			}
			this.metrics = operatorMetricGroup;
		} catch (Exception e) {
			LOG.warn("An error occurred while instantiating task metrics.", e);
			this.metrics = UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup();
			this.output = output;
		}

		try {
			Configuration taskManagerConfig = environment.getTaskManagerInfo().getConfiguration();
			int historySize = taskManagerConfig.getInteger(MetricOptions.LATENCY_HISTORY_SIZE);
			if (historySize <= 0) {
				LOG.warn("{} has been set to a value equal or below 0: {}. Using default.", MetricOptions.LATENCY_HISTORY_SIZE, historySize);
				historySize = MetricOptions.LATENCY_HISTORY_SIZE.defaultValue();
			}

			final String configuredGranularity = taskManagerConfig.getString(MetricOptions.LATENCY_SOURCE_GRANULARITY);
			LatencyStats.Granularity granularity;
			try {
				granularity = LatencyStats.Granularity.valueOf(configuredGranularity.toUpperCase(Locale.ROOT));
			} catch (IllegalArgumentException iae) {
				granularity = LatencyStats.Granularity.OPERATOR;
				LOG.warn(
					"Configured value {} option for {} is invalid. Defaulting to {}.",
					configuredGranularity,
					MetricOptions.LATENCY_SOURCE_GRANULARITY.key(),
					granularity);
			}
			TaskManagerJobMetricGroup jobMetricGroup = this.metrics.parent().parent();
			// 用于可视化的显示 flink 网络中的延迟，不深究了
			this.latencyStats = new LatencyStats(jobMetricGroup.addGroup("latency"),
				historySize,
				container.getIndexInSubtaskGroup(),
				getOperatorID(),
				granularity);
		} catch (Exception e) {
			LOG.warn("An error occurred while instantiating latency metrics.", e);
			this.latencyStats = new LatencyStats(
				UnregisteredMetricGroups.createUnregisteredTaskManagerJobMetricGroup().addGroup("latency"),
				1,
				0,
				new OperatorID(),
				LatencyStats.Granularity.SINGLE);
		}

		this.runtimeContext = new StreamingRuntimeContext(this, environment, container.getAccumulatorMap());

		// 从 StreamConfig 中获取 stateKeySelector，生成 JobGraph 的时候写入 StreamConfig
		stateKeySelector1 = config.getStatePartitioner(0, getUserCodeClassloader());
		stateKeySelector2 = config.getStatePartitioner(1, getUserCodeClassloader());
	}

	@Override
	public MetricGroup getMetricGroup() {
		return metrics;
	}

	@Override
	/**
	 * 初始化操作符状态
	 */
	public final void initializeState() throws Exception {

		final TypeSerializer<?> keySerializer = config.getStateKeySerializer(getUserCodeClassloader());

		// 获取 containingTask，setup 方法中定义的，在 operatorChain.java 中会调用 setup 方法
		final StreamTask<?, ?> containingTask =
			Preconditions.checkNotNull(getContainingTask());
		// 获取 StreamTask 的关闭策略
		final CloseableRegistry streamTaskCloseableRegistry =
			Preconditions.checkNotNull(containingTask.getCancelables());
		// 获取 StreamTask 状态初始化工具
		final StreamTaskStateInitializer streamTaskStateManager =
			Preconditions.checkNotNull(containingTask.createStreamTaskStateInitializer());
		// 获取流操作符状态上下文
		final StreamOperatorStateContext context =
			streamTaskStateManager.streamOperatorStateContext(
				getOperatorID(),
				getClass().getSimpleName(),
				this,
				keySerializer,
				streamTaskCloseableRegistry,
				metrics);

		this.operatorStateBackend = context.operatorStateBackend();
		this.keyedStateBackend = context.keyedStateBackend();

		if (keyedStateBackend != null) {
			this.keyedStateStore = new DefaultKeyedStateStore(keyedStateBackend, getExecutionConfig());
		}

		timeServiceManager = context.internalTimerServiceManager();

		CloseableIterable<KeyGroupStatePartitionStreamProvider> keyedStateInputs = context.rawKeyedStateInputs();
		CloseableIterable<StatePartitionStreamProvider> operatorStateInputs = context.rawOperatorStateInputs();

		try {
			StateInitializationContext initializationContext = new StateInitializationContextImpl(
				context.isRestored(), // information whether we restore or start for the first time
				operatorStateBackend, // access to operator state backend
				keyedStateStore, // access to keyed state backend
				keyedStateInputs, // access to keyed state stream
				operatorStateInputs); // access to operator state stream

			initializeState(initializationContext);
		} finally {
			closeFromRegistry(operatorStateInputs, streamTaskCloseableRegistry);
			closeFromRegistry(keyedStateInputs, streamTaskCloseableRegistry);
		}
	}

	// 安全释放资源
	private static void closeFromRegistry(Closeable closeable, CloseableRegistry registry) {
		if (registry.unregisterCloseable(closeable)) {
			IOUtils.closeQuietly(closeable);
		}
	}

	/**
	 * This method is called immediately before any elements are processed, it should contain the
	 * operator's initialization logic, e.g. state initialization.
	 *
	 * <p>The default implementation does nothing.
	 *
	 * @throws Exception An exception in this method causes the operator to fail.
	 */
	/**
	 * 这个方法需要在处理第一个元素之前被调用
	 */
	@Override
	public void open() throws Exception {}

	/**
	 * This method is called after all records have been added to the operators via the methods
	 * {@link OneInputStreamOperator#processElement(StreamRecord)}, or
	 * {@link TwoInputStreamOperator#processElement1(StreamRecord)} and
	 * {@link TwoInputStreamOperator#processElement2(StreamRecord)}.
	 *
	 * <p>The method is expected to flush all remaining buffered data. Exceptions during this flushing
	 * of buffered should be propagated, in order to cause the operation to be recognized asa failed,
	 * because the last data items are not processed properly.
	 *
	 * @throws Exception An exception in this method causes the operator to fail.
	 */
	/**
	 * 这个方法在所有的元素被操作符处理之后被调用
	 *
	 * 这个方法需要 flush 缓冲区中的所有的元素，在 flushing 过程中抛出的异常需要被传播，因为需要让
	 * 这个操作被认为失败了，毕竟最后缓冲区中的元素没有被合适的处理
	 */
	@Override
	public void close() throws Exception {}

	/**
	 * This method is called at the very end of the operator's life, both in the case of a successful
	 * completion of the operation, and in the case of a failure and canceling.
	 *
	 * <p>This method is expected to make a thorough effort to release all resources
	 * that the operator has acquired.
	 */
	/**
	 * 无论是操作成功还是操作失败，这个方法需要在操作符生命的最后被调用
	 * 
	 * 这个方法需要彻底释放操作符申请的所有资源
	 */
	@Override
	public void dispose() throws Exception {

		Exception exception = null;

		StreamTask<?, ?> containingTask = getContainingTask();  // 获取 StreamTask
		CloseableRegistry taskCloseableRegistry = containingTask != null ?
			containingTask.getCancelables() :
			null;

		try {
			if (taskCloseableRegistry == null ||
				taskCloseableRegistry.unregisterCloseable(operatorStateBackend)) {
				operatorStateBackend.close();
			}
		} catch (Exception e) {
			exception = e;
		}

		try {
			if (taskCloseableRegistry == null ||
				taskCloseableRegistry.unregisterCloseable(keyedStateBackend)) {
				keyedStateBackend.close();
			}
		} catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(e, exception);
		}

		try {
			if (operatorStateBackend != null) {
				operatorStateBackend.dispose();
			}
		} catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(e, exception);
		}

		try {
			if (keyedStateBackend != null) {
				keyedStateBackend.dispose();
			}
		} catch (Exception e) {
			exception = ExceptionUtils.firstOrSuppressed(e, exception);
		}

		if (exception != null) {
			throw exception;
		}
	}

	@Override
	public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
		// the default implementation does nothing and accepts the checkpoint
		// this is purely for subclasses to override
	}

	@Override
	public final OperatorSnapshotFutures snapshotState(long checkpointId, long timestamp, CheckpointOptions checkpointOptions,
			CheckpointStreamFactory factory) throws Exception {

		KeyGroupRange keyGroupRange = null != keyedStateBackend ?
				keyedStateBackend.getKeyGroupRange() : KeyGroupRange.EMPTY_KEY_GROUP_RANGE;

		OperatorSnapshotFutures snapshotInProgress = new OperatorSnapshotFutures();

		try (StateSnapshotContextSynchronousImpl snapshotContext = new StateSnapshotContextSynchronousImpl(
				checkpointId,
				timestamp,
				factory,
				keyGroupRange,
				getContainingTask().getCancelables())) {

			snapshotState(snapshotContext);

			snapshotInProgress.setKeyedStateRawFuture(snapshotContext.getKeyedStateStreamFuture());
			snapshotInProgress.setOperatorStateRawFuture(snapshotContext.getOperatorStateStreamFuture());

			if (null != operatorStateBackend) {
				snapshotInProgress.setOperatorStateManagedFuture(
					operatorStateBackend.snapshot(checkpointId, timestamp, factory, checkpointOptions));
			}

			if (null != keyedStateBackend) {
				snapshotInProgress.setKeyedStateManagedFuture(
					keyedStateBackend.snapshot(checkpointId, timestamp, factory, checkpointOptions));
			}
		} catch (Exception snapshotException) {
			try {
				snapshotInProgress.cancel();
			} catch (Exception e) {
				snapshotException.addSuppressed(e);
			}

			String snapshotFailMessage = "Could not complete snapshot " + checkpointId + " for operator " +
				getOperatorName() + ".";

			if (!getContainingTask().isCanceled()) {
				LOG.info(snapshotFailMessage, snapshotException);
			}
			throw new Exception(snapshotFailMessage, snapshotException);
		}

		return snapshotInProgress;
	}

	/**
	 * Stream operators with state, which want to participate in a snapshot need to override this hook method.
	 *
	 * @param context context that provides information and means required for taking a snapshot
	 */
	public void snapshotState(StateSnapshotContext context) throws Exception {
		final KeyedStateBackend<?> keyedStateBackend = getKeyedStateBackend();
		//TODO all of this can be removed once heap-based timers are integrated with RocksDB incremental snapshots
		if (keyedStateBackend instanceof AbstractKeyedStateBackend &&
			((AbstractKeyedStateBackend<?>) keyedStateBackend).requiresLegacySynchronousTimerSnapshots()) {

			KeyedStateCheckpointOutputStream out;

			try {
				out = context.getRawKeyedOperatorStateOutput();
			} catch (Exception exception) {
				throw new Exception("Could not open raw keyed operator state stream for " +
					getOperatorName() + '.', exception);
			}

			try {
				KeyGroupsList allKeyGroups = out.getKeyGroupList();
				for (int keyGroupIdx : allKeyGroups) {
					out.startNewKeyGroup(keyGroupIdx);

					timeServiceManager.snapshotStateForKeyGroup(
						new DataOutputViewStreamWrapper(out), keyGroupIdx);
				}
			} catch (Exception exception) {
				throw new Exception("Could not write timer service of " + getOperatorName() +
					" to checkpoint state stream.", exception);
			} finally {
				try {
					out.close();
				} catch (Exception closeException) {
					LOG.warn("Could not close raw keyed operator state stream for {}. This " +
						"might have prevented deleting some state data.", getOperatorName(), closeException);
				}
			}
		}
	}

	/**
	 * Stream operators with state which can be restored need to override this hook method.
	 *
	 * @param context context that allows to register different states.
	 */
	/**
	 * 有状态的流操作符能够通过重写这个钩子方法来恢复状态
	 */
	public void initializeState(StateInitializationContext context) throws Exception {

	}

	@Override
	/**
	 * 通知检查点作业完成
	 */
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		if (keyedStateBackend != null) {
			keyedStateBackend.notifyCheckpointComplete(checkpointId);
		}
	}

	// ------------------------------------------------------------------------
	//  Properties and Services
	// ------------------------------------------------------------------------

	/**
	 * Gets the execution config defined on the execution environment of the job to which this
	 * operator belongs.
	 *
	 * @return The job's execution config.
	 */
	/**
	 * 获取 StreamTask 的执行配置
	 */
	public ExecutionConfig getExecutionConfig() {
		return container.getExecutionConfig();
	}

	/**
	 * 获取操作符的 StreamConfig
	 */
	public StreamConfig getOperatorConfig() {
		return config;
	}

	public StreamTask<?, ?> getContainingTask() {
		return container;
	}

	public ClassLoader getUserCodeClassloader() {
		return container.getUserCodeClassLoader();
	}

	/**
	 * Return the operator name. If the runtime context has been set, then the task name with
	 * subtask index is returned. Otherwise, the simple class name is returned.
	 *
	 * @return If runtime context is set, then return task name with subtask index. Otherwise return
	 * 			simple class name.
	 */
	protected String getOperatorName() {
		if (runtimeContext != null) {
			return runtimeContext.getTaskNameWithSubtasks();
		} else {
			return getClass().getSimpleName();
		}
	}

	/**
	 * Returns a context that allows the operator to query information about the execution and also
	 * to interact with systems such as broadcast variables and managed state. This also allows
	 * to register timers.
	 */
	/**
	 * 返回一个上下文，该上下文允许操作符查询有关执行的信息
	 * 并与系统（如广播变量和托管状态）进行交互。这也允许注册定时器
	 */
	public StreamingRuntimeContext getRuntimeContext() {
		return runtimeContext;
	}

	@SuppressWarnings("unchecked")
	public <K> KeyedStateBackend<K> getKeyedStateBackend() {
		return (KeyedStateBackend<K>) keyedStateBackend;
	}

	public OperatorStateBackend getOperatorStateBackend() {
		return operatorStateBackend;
	}

	/**
	 * Returns the {@link ProcessingTimeService} responsible for getting  the current
	 * processing time and registering timers.
	 */
	/**
	 * 获取 ProcessingTimeService，用来得到当前的进程时间以及注册定时器
	 */
	protected ProcessingTimeService getProcessingTimeService() {
		return container.getProcessingTimeService();
	}

	/**
	 * Creates a partitioned state handle, using the state backend configured for this task.
	 *
	 * @throws IllegalStateException Thrown, if the key/value state was already initialized.
	 * @throws Exception Thrown, if the state backend cannot create the key/value state.
	 */
	/**
	 * 使用为此任务配置的状态后端创建分区状态句柄
	 */
	protected <S extends State> S getPartitionedState(StateDescriptor<S, ?> stateDescriptor) throws Exception {
		return getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, stateDescriptor);
	}

	/**
	 * 如果状态存在已经存在，直接返回，否则，先创建再返回
	 */
	protected <N, S extends State, T> S getOrCreateKeyedState(
			TypeSerializer<N> namespaceSerializer,
			StateDescriptor<S, T> stateDescriptor) throws Exception {

		if (keyedStateStore != null) {
			return keyedStateBackend.getOrCreateKeyedState(namespaceSerializer, stateDescriptor);
		}
		else {
			throw new IllegalStateException("Cannot create partitioned state. " +
					"The keyed state backend has not been set." +
					"This indicates that the operator is not partitioned/keyed.");
		}
	}

	/**
	 * Creates a partitioned state handle, using the state backend configured for this task.
	 *
	 * @throws IllegalStateException Thrown, if the key/value state was already initialized.
	 * @throws Exception Thrown, if the state backend cannot create the key/value state.
	 */
	/**
	 * 使用为此任务配置的状态后端创建分区状态句柄
	 */
	protected <S extends State, N> S getPartitionedState(
			N namespace,
			TypeSerializer<N> namespaceSerializer,
			StateDescriptor<S, ?> stateDescriptor) throws Exception {

		/*
	    TODO: NOTE: This method does a lot of work caching / retrieving states just to update the namespace.
	    This method should be removed for the sake of namespaces being lazily fetched from the keyed
	    state backend, or being set on the state directly.
	    */

		if (keyedStateStore != null) {
			return keyedStateBackend.getPartitionedState(namespace, namespaceSerializer, stateDescriptor);
		} else {
			throw new RuntimeException("Cannot create partitioned state. The keyed state " +
				"backend has not been set. This indicates that the operator is not " +
				"partitioned/keyed.");
		}
	}

	@Override
	@SuppressWarnings({"unchecked", "rawtypes"})
	// 设置 currentKey，record 来自第一个 input
	public void setKeyContextElement1(StreamRecord record) throws Exception {
		setKeyContextElement(record, stateKeySelector1);
	}

	@Override
	// 设置 currentKey，record 来自第二个 input
	@SuppressWarnings({"unchecked", "rawtypes"})
	public void setKeyContextElement2(StreamRecord record) throws Exception {
		setKeyContextElement(record, stateKeySelector2);
	}

	private <T> void setKeyContextElement(StreamRecord<T> record, KeySelector<T, ?> selector) throws Exception {
		if (selector != null) {
			// 利用 KeySelector 计算 key，然后设置 currentKey
			Object key = selector.getKey(record.getValue());
			setCurrentKey(key);
		}
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	public void setCurrentKey(Object key) {
		if (keyedStateBackend != null) {
			try {
				// need to work around type restrictions
				@SuppressWarnings("unchecked,rawtypes")
				AbstractKeyedStateBackend rawBackend = (AbstractKeyedStateBackend) keyedStateBackend;

				rawBackend.setCurrentKey(key);
			} catch (Exception e) {
				throw new RuntimeException("Exception occurred while setting the current key context.", e);
			}
		}
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	public Object getCurrentKey() {
		if (keyedStateBackend != null) {
			return keyedStateBackend.getCurrentKey();
		} else {
			throw new UnsupportedOperationException("Key can only be retrieved on KeyedStream.");
		}
	}

	public KeyedStateStore getKeyedStateStore() {
		return keyedStateStore;
	}

	// ------------------------------------------------------------------------
	//  Context and chaining properties
	// ------------------------------------------------------------------------

	@Override
	/**
	 * 设置链式策略
	 */
	public final void setChainingStrategy(ChainingStrategy strategy) {
		this.chainingStrategy = strategy;
	}

	@Override
	/**
	 * 获取链式策略
	 */
	public final ChainingStrategy getChainingStrategy() {
		return chainingStrategy;
	}


	// ------------------------------------------------------------------------
	//  Metrics
	// ------------------------------------------------------------------------

	// ------- One input stream
	public void processLatencyMarker(LatencyMarker latencyMarker) throws Exception {
		reportOrForwardLatencyMarker(latencyMarker);
	}

	// ------- Two input stream
	public void processLatencyMarker1(LatencyMarker latencyMarker) throws Exception {
		reportOrForwardLatencyMarker(latencyMarker);
	}

	public void processLatencyMarker2(LatencyMarker latencyMarker) throws Exception {
		reportOrForwardLatencyMarker(latencyMarker);
	}

	protected void reportOrForwardLatencyMarker(LatencyMarker marker) {
		// all operators are tracking latencies
		this.latencyStats.reportLatency(marker);

		// everything except sinks forwards latency markers
		this.output.emitLatencyMarker(marker);
	}

	// ----------------------- Helper classes -----------------------

	/**
	 * Wrapping {@link Output} that updates metrics on the number of emitted elements.
	 */
	public static class CountingOutput<OUT> implements Output<StreamRecord<OUT>> {
		private final Output<StreamRecord<OUT>> output;
		private final Counter numRecordsOut;

		public CountingOutput(Output<StreamRecord<OUT>> output, Counter counter) {
			this.output = output;
			this.numRecordsOut = counter;
		}

		@Override
		public void emitWatermark(Watermark mark) {
			output.emitWatermark(mark);
		}

		@Override
		public void emitLatencyMarker(LatencyMarker latencyMarker) {
			output.emitLatencyMarker(latencyMarker);
		}

		@Override
		public void collect(StreamRecord<OUT> record) {
			numRecordsOut.inc();
			output.collect(record);
		}

		@Override
		public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {
			numRecordsOut.inc();
			output.collect(outputTag, record);
		}

		@Override
		public void close() {
			output.close();
		}
	}

	// ------------------------------------------------------------------------
	//  Watermark handling
	// ------------------------------------------------------------------------

	/**
	 * Returns a {@link InternalTimerService} that can be used to query current processing time
	 * and event time and to set timers. An operator can have several timer services, where
	 * each has its own namespace serializer. Timer services are differentiated by the string
	 * key that is given when requesting them, if you call this method with the same key
	 * multiple times you will get the same timer service instance in subsequent requests.
	 *
	 * 返回一个 InternalTimerService，能够用来请求当前进程时间和事件时间，也能够设置定时器
	 * 一个操作符可以有多个时间服务，每个时间服务的命名空间不相同
	 * 计时器服务通过请求时给定的字符串键来区分，如果多次使用相同的键调用此方法
	 * 则在随后的请求中将获得相同的计时器服务实例
	 *
	 * <p>Timers are always scoped to a key, the currently active key of a keyed stream operation.
	 * When a timer fires, this key will also be set as the currently active key.
	 *
	 * 定时器总是在一个 key 的范围内，只有 KeyedStream 能够设置定时器
	 *
	 * <p>Each timer has attached metadata, the namespace. Different timer services
	 * can have a different namespace type. If you don't need namespace differentiation you
	 * can use {@link VoidNamespaceSerializer} as the namespace serializer.
	 *
	 * 每个定时器都有一个命名空间，如果你不需要的话，可以使用 VoidNamespaceSerializer
	 *
	 * @param name The name of the requested timer service. If no service exists under the given
	 *             name a new one will be created and returned.
	 * @param namespaceSerializer {@code TypeSerializer} for the timer namespace.
	 * @param triggerable The {@link Triggerable} that should be invoked when timers fire
	 *
	 * @param <N> The type of the timer namespace.
	 */
	/**
	 * 获取内部时间服务
	 */
	public <K, N> InternalTimerService<N> getInternalTimerService(
			String name,
			TypeSerializer<N> namespaceSerializer,
			Triggerable<K, N> triggerable) {

		checkTimerServiceInitialization();

		// the following casting is to overcome type restrictions.
		KeyedStateBackend<K> keyedStateBackend = getKeyedStateBackend();
		TypeSerializer<K> keySerializer = keyedStateBackend.getKeySerializer();
		InternalTimeServiceManager<K> keyedTimeServiceHandler = (InternalTimeServiceManager<K>) timeServiceManager;
		TimerSerializer<K, N> timerSerializer = new TimerSerializer<>(keySerializer, namespaceSerializer);
		return keyedTimeServiceHandler.getInternalTimerService(name, timerSerializer, triggerable);
	}

	/**
	 * 处理 watermark
	 * output 将 watermark 传递给下游的操作符
	 * timeServiceManager.advanceWatermark 通知 eventTimeTimer
	 */
	public void processWatermark(Watermark mark) throws Exception {
		if (timeServiceManager != null) {
			timeServiceManager.advanceWatermark(mark);
		}
		output.emitWatermark(mark);
	}

	/**
	 * 检查时间服务的条件是否满足
	 * 定时器只能被用于 keyed 操作符
	 * 必须初始化 timeServiceManager
	 */
	private void checkTimerServiceInitialization() {
		if (getKeyedStateBackend() == null) {
			throw new UnsupportedOperationException("Timers can only be used on keyed operators.");
		} else if (timeServiceManager == null) {
			throw new RuntimeException("The timer service has not been initialized.");
		}
	}

	/**
	 * 处理 input1 来的 watermark
	 */
	public void processWatermark1(Watermark mark) throws Exception {
		input1Watermark = mark.getTimestamp();
		long newMin = Math.min(input1Watermark, input2Watermark);
		if (newMin > combinedWatermark) {
			combinedWatermark = newMin;
			processWatermark(new Watermark(combinedWatermark));
		}
	}

	/**
	 * 处理 input2 来的 watermark
	 */
	public void processWatermark2(Watermark mark) throws Exception {
		input2Watermark = mark.getTimestamp();
		long newMin = Math.min(input1Watermark, input2Watermark);
		if (newMin > combinedWatermark) {
			combinedWatermark = newMin;
			processWatermark(new Watermark(combinedWatermark));
		}
	}

	@Override
	/**
	 * 获取操作符的 id，由 hash 生成
	 */
	public OperatorID getOperatorID() {
		return config.getOperatorID();
	}

	@VisibleForTesting
	public int numProcessingTimeTimers() {
		return timeServiceManager == null ? 0 :
			timeServiceManager.numProcessingTimeTimers();
	}

	@VisibleForTesting
	public int numEventTimeTimers() {
		return timeServiceManager == null ? 0 :
			timeServiceManager.numEventTimeTimers();
	}
}
