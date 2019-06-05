# flink 的窗口 —— 窗口函数

上篇文章我们介绍了窗口操作符，窗口操作符本质上就是一个流操作符，也是通过 processElement 来处理流元素，只是一般的操作符执行窗口函数之后就 emit 结果到下游了，窗口操作符通过 trigger 设置定时器，当定时器触发的时候，取出窗口中保存的数据，执行窗口函数并 emit 结果到下游（其实窗口操作符有些类似于之前讲过的异步操作符）

窗口流（由 DataStream 转换得到，下一篇文章会介绍）类似于之前讲过的 KeyedStream，提供 reduce、fold、sum 等函数对窗口进行聚合操作，这是理解窗口函数的基础

我们知道窗口操作符分为 WindowOperator 和 EvictingWindowOperator，EvictingWindowOperator 会在 emitWindowContents 前后 remove 部分窗口中的元素。当使用 WindowOperator 的时候，窗口在接受到一个元素之后，就会在内部执行聚合操作，当 emitWindowContents 的时候，直接将最后聚合好的数据 emit 给下游即可，当使用 EvictingWindowOperator 的时候，由于 Evitor 的存在，我们需要存储所有的元素，这样我们才能在 emitWindowContents 中 remove 部分元素，因此我们需要在 emitWindowContents 的时候手动去执行聚合操作

基于上述情况，flink 将窗口函数分为窗口操作符侧的窗口函数以及窗口流侧的窗口函数

## 窗口操作符侧的窗口函数（内部函数）

* InternalWindowFunction

	InternalWindowFunction 是一个接口，内部函数都实现了这个接口
	
	```java
	public interface InternalWindowFunction<IN, OUT, KEY, W extends Window> extends Function {
		/**
		 * 执行窗口，输出一个或多个元素
		 */
		void process(KEY key, W window, InternalWindowContext context, IN input, Collector<OUT> out) throws Exception;
	
		/**
		 * 当窗口被清洗的时候删除 context 内的所有状态
		 */
		void clear(W window, InternalWindowContext context) throws Exception;
		
		/**
		 * InternalWindowFunction 的上下文
		 */
		interface InternalWindowContext extends java.io.Serializable {
			long currentProcessingTime();
	
			long currentWatermark();
	
			KeyedStateStore windowState();
	
			KeyedStateStore globalState();
	
			<X> void output(OutputTag<X> outputTag, X value);
		}
	}
	```

* InternalProcessWindowContext

	InternalProcessWindowContext 继承 `ProcessWindowFunction<IN, OUT, KEY, W>.Context`，用于给 ProcessWindowFunction 提供上下文环境，InternalProcessWindowContext 内部包裹一个 `InternalWindowFunction.InternalWindowContext`，即 WindowOperator 中的 processContext。InternalProcessWindowContext 能够获取当前的进程时间、Watermark、获取 State 以及操作偏侧输出
	
	```java
	public class InternalProcessWindowContext<IN, OUT, KEY, W extends Window>
		extends ProcessWindowFunction<IN, OUT, KEY, W>.Context {
	
		W window;
		InternalWindowFunction.InternalWindowContext internalContext;
	
		InternalProcessWindowContext(ProcessWindowFunction<IN, OUT, KEY, W> function) {
			function.super();
		}
	
		@Override
		public W window() {
			return window;
		}
	
		@Override
		public long currentProcessingTime() {
			return internalContext.currentProcessingTime();
		}
	
		@Override
		public long currentWatermark() {
			return internalContext.currentWatermark();
		}
	
		@Override
		public KeyedStateStore windowState() {
			return internalContext.windowState();
		}
	
		@Override
		public KeyedStateStore globalState() {
			return internalContext.globalState();
		}
	
		@Override
		public <X> void output(OutputTag<X> outputTag, X value) {
			internalContext.output(outputTag, value);
		}
	}
	```

* InternalSingleValueWindowFunction

	InternalSingleValueWindowFunction 服务于 WindowOpertor，包裹一个 WindowFunction，当 process 方法被 WindowOpertor 的 emitWindowContents 调用的时候，直接将 WindowOpertor **最后的聚合状态**包装成一个 Collection，再调用 WindowFunction 的方法进行输出

* InternalSingleValueProcessWindowFunction

	InternalSingleValueProcessWindowFunction 和 InternalSingleValueWindowFunction 的区别类似于 DataStream 中 FlatMapFunction 和 ProcessFunction 的区别，InternalSingleValueProcessWindowFunction 内部有一个 InternalProcessWindowContext 字段，能够访问上下文

	```java
	public final class InternalSingleValueProcessWindowFunction<IN, OUT, KEY, W extends Window>
			extends WrappingFunction<ProcessWindowFunction<IN, OUT, KEY, W>>
			implements InternalWindowFunction<IN, OUT, KEY, W> {

		private final InternalProcessWindowContext<IN, OUT, KEY, W> ctx;
	
		public InternalSingleValueProcessWindowFunction(ProcessWindowFunction<IN, OUT, KEY, W> wrappedFunction) {
			super(wrappedFunction);
			ctx = new InternalProcessWindowContext<>(wrappedFunction);
		}
	
		@Override
		public void process(KEY key, final W window, final InternalWindowContext context, IN input, Collector<OUT> out) throws Exception {
			this.ctx.window = window;
			this.ctx.internalContext = context;
	
			ProcessWindowFunction<IN, OUT, KEY, W> wrappedFunction = this.wrappedFunction;
			wrappedFunction.process(key, ctx, Collections.singletonList(input), out);
		}
	
		@Override
		public void clear(final W window, final InternalWindowContext context) throws Exception {
			this.ctx.window = window;
			this.ctx.internalContext = context;
	
			ProcessWindowFunction<IN, OUT, KEY, W> wrappedFunction = this.wrappedFunction;
			wrappedFunction.clear(ctx);
		}
	
		@Override
		public RuntimeContext getRuntimeContext() {
			throw new RuntimeException("This should never be called.");
		}
	
		@Override
		public IterationRuntimeContext getIterationRuntimeContext() {
			throw new RuntimeException("This should never be called.");
		}
	}
	```

* InternalIterableWindowFunction
	
	InternalIterableWindowFunction 服务于 EvictingWindowOperator，包裹一个 WindowFunction，当 EvictingWindowOperator 的 emitWindowContents 执行完驱逐操作之后，会调用 process 方法，传入当前窗口中元素组成的迭代器，process 将迭代器传给 WindowFunction 进行输出
	
	```java
	public final class InternalIterableWindowFunction<IN, OUT, KEY, W extends Window>
			extends WrappingFunction<WindowFunction<IN, OUT, KEY, W>>
			implements InternalWindowFunction<Iterable<IN>, OUT, KEY, W> {

		public InternalIterableWindowFunction(WindowFunction<IN, OUT, KEY, W> wrappedFunction) {
			super(wrappedFunction);
		}
	
		@Override
		public void process(KEY key, W window, InternalWindowContext context, Iterable<IN> input, Collector<OUT> out) throws Exception {
			wrappedFunction.apply(key, window, input, out);
		}
	
		@Override
		public void clear(W window, InternalWindowContext context) throws Exception {
	
		}
	
		@Override
		public RuntimeContext getRuntimeContext() {
			throw new RuntimeException("This should never be called.");
		}
	
		@Override
		public IterationRuntimeContext getIterationRuntimeContext() {
			throw new RuntimeException("This should never be called.");
		}
	}
	```

* InternalIterableProcessWindowFunction

	InternalIterableProcessWindowFunction 和 InternalSingleValueProcessWindowFunction 类似，内部都有一个 InternalProcessWindowContext 字段用来给 ProcessWindowFunction 提供上下文
	
	```java
	public final class InternalIterableProcessWindowFunction<IN, OUT, KEY, W extends Window>
			extends WrappingFunction<ProcessWindowFunction<IN, OUT, KEY, W>>
			implements InternalWindowFunction<Iterable<IN>, OUT, KEY, W> {

		private final InternalProcessWindowContext<IN, OUT, KEY, W> ctx;
	
		public InternalIterableProcessWindowFunction(ProcessWindowFunction<IN, OUT, KEY, W> wrappedFunction) {
			super(wrappedFunction);
			this.ctx = new InternalProcessWindowContext<>(wrappedFunction);
		}
	
		@Override
		public void process(KEY key, final W window, final InternalWindowContext context, Iterable<IN> input, Collector<OUT> out) throws Exception {
			this.ctx.window = window;
			this.ctx.internalContext = context;
			ProcessWindowFunction<IN, OUT, KEY, W> wrappedFunction = this.wrappedFunction;
			wrappedFunction.process(key, ctx, input, out);
		}
	
		@Override
		public void clear(final W window, final InternalWindowContext context) throws Exception {
			this.ctx.window = window;
			this.ctx.internalContext = context;
			ProcessWindowFunction<IN, OUT, KEY, W> wrappedFunction = this.wrappedFunction;
			wrappedFunction.clear(ctx);
		}
	
		@Override
		public RuntimeContext getRuntimeContext() {
			throw new RuntimeException("This should never be called.");
		}
	
		@Override
		public IterationRuntimeContext getIterationRuntimeContext() {
			throw new RuntimeException("This should never be called.");
		}
	}
	```

* InternalAggregateProcessWindowFunction

	InternalAggregateProcessWindowFunction 服务于 EvictingWindowOperator，内部包裹一个 ProcessWindowFunction 以及一个 AggregateFunction，当 process 方法被调用的时候，先创建一个 Accumulator，然后对列表中所有的元素应用 AggregateFunction，最后将聚合的结果传递给 ProcessWindowFunction，处理后 emit 结果给下游

	```java
	public final class InternalAggregateProcessWindowFunction<T, ACC, V, R, K, W extends Window>
			extends WrappingFunction<ProcessWindowFunction<V, R, K, W>>
			implements InternalWindowFunction<Iterable<T>, R, K, W> {

		private final AggregateFunction<T, ACC, V> aggFunction;
	
		private final InternalProcessWindowContext<V, R, K, W> ctx;
	
		public InternalAggregateProcessWindowFunction(
				AggregateFunction<T, ACC, V> aggFunction,
				ProcessWindowFunction<V, R, K, W> windowFunction) {
			super(windowFunction);
			this.aggFunction = aggFunction;
			this.ctx = new InternalProcessWindowContext<>(windowFunction);
		}
	
		@Override
		public void process(K key, final W window, final InternalWindowContext context, Iterable<T> input, Collector<R> out) throws Exception {
			ACC acc = aggFunction.createAccumulator();
	
			for (T val : input) {
				acc = aggFunction.add(val, acc);
			}
	
			this.ctx.window = window;
			this.ctx.internalContext = context;
			ProcessWindowFunction<V, R, K, W> wrappedFunction = this.wrappedFunction;
			wrappedFunction.process(key, ctx, Collections.singletonList(aggFunction.getResult(acc)), out);
		}
	
		@Override
		public void clear(final W window, final InternalWindowContext context) throws Exception {
			this.ctx.window = window;
			this.ctx.internalContext = context;
			ProcessWindowFunction<V, R, K, W> wrappedFunction = this.wrappedFunction;
			wrappedFunction.clear(ctx);
		}
	
		@Override
		public RuntimeContext getRuntimeContext() {
			throw new RuntimeException("This should never be called.");
		}
	
		@Override
		public IterationRuntimeContext getIterationRuntimeContext() {
			throw new RuntimeException("This should never be called.");
		}
	}
	```

## 窗口流侧的窗口函数（外部函数）

* WindowFunction

	WindowFunction 是窗口函数的基本接口，apply 方法用来计算并将结果 emit 给下游操作符
	
	```java
	public interface WindowFunction<IN, OUT, KEY, W extends Window> extends Function, Serializable {
		/**
		 * 执行窗口并且输出零个或者多个元素
		 */
		void apply(KEY key, W window, Iterable<IN> input, Collector<OUT> out) throws Exception;
	}
	```

* ProcessWindowFunction

	ProcessWindowFunction 使用上下文获取额外信息，是窗口计算的基本抽象类，process 方法用来计算窗口，然后输出零个或多个元素，由于 ProcessWindowFunction 可以通过上下文获取 State，因此提供 clear 方法，用于在窗口销毁的时候删除 Context 中的所有状态

	```java
	public abstract class ProcessWindowFunction<IN, OUT, KEY, W extends Window> extends AbstractRichFunction {
		/**
		 * 计算窗口，然后输出零个或多个元素
		 */
		public abstract void process(KEY key, Context context, Iterable<IN> elements, Collector<OUT> out) throws Exception;
	
		/**
		 * 当窗口被清洗的时候，删除 Context 中的所有状态
		 */
		public void clear(Context context) throws Exception {}

		/**
		 * 掌握窗口元数据的上下文
		 */
		public abstract class Context implements java.io.Serializable {
			/**
			 * 返回窗口是否正在被计算
			 */
			public abstract W window();
	
			/**
			 * 返回当前的进程时间
			 */
			public abstract long currentProcessingTime();

			/**
			 * 返回当前的 Watermark
			 */
			public abstract long currentWatermark();
	
			/**
			 * 每个 key，每个窗口的状态获取器
			 */
			public abstract KeyedStateStore windowState();

			/**
			 * 每个 key 的全局状态获取器
			 */
			public abstract KeyedStateStore globalState();

			/**
			 * 侧边输出
			 */
			public abstract <X> void output(OutputTag<X> outputTag, X value);
		}
	}
	```

* InternalProcessApplyWindowContext

	InternalProcessApplyWindowContext 继承 ProcessWindowFunction<IN, OUT, KEY, W>.Context，用于提供 ProcessWindowFunction 中的 Context，InternalProcessApplyWindowContext 类似于窗口操作符侧的 InternalProcessWindowContext，其实 InternalProcessApplyWindowContext 内部也就是包裹了一个 InternalProcessWindowContext，个人觉得这块的设计有些冗余

	```java
	public class InternalProcessApplyWindowContext<IN, OUT, KEY, W extends Window>
		extends ProcessWindowFunction<IN, OUT, KEY, W>.Context {
	
		W window;
		ProcessWindowFunction.Context context;
	
		InternalProcessApplyWindowContext(ProcessWindowFunction<IN, OUT, KEY, W> function) {
			function.super();
		}
	
		@Override
		public W window() {
			return window;
		}
	
		@Override
		public long currentProcessingTime() {
			return context.currentProcessingTime();
		}
	
		@Override
		public long currentWatermark() {
			return context.currentWatermark();
		}
	
		public KeyedStateStore windowState() {
			return context.windowState();
		}
	
		@Override
		public KeyedStateStore globalState() {
			return context.globalState();
		}
	
		@Override
		public <X> void output(OutputTag<X> outputTag, X value) {
			context.output(outputTag, value);
		}
	}
	```

* PassThroughWindowFunction

	PassThroughWindowFunction 用于在用户未提供 WindowFunction 的时候充当 WindowFunction，提供一个 apply，直接将输入 emit 到下游

	```java
	public class PassThroughWindowFunction<K, W extends Window, T> implements WindowFunction<T, T, K, W> {
	
		private static final long serialVersionUID = 1L;
	
		@Override
		public void apply(K k, W window, Iterable<T> input, Collector<T> out) throws Exception {
			for (T in: input) {
				out.collect(in);
			}
		}
	}
	```
	
* ReduceApplyWindowFunction

	当使用 EvictingWindowOperator 的时候，我们需要保存所有的元素集合，需要自行调用聚合函数进行聚合，这个时候，使用 ReduceApplyWindowFunction 包裹 reduceFunction 和 windowFunction，当 apply 执行的时候，先使用 reduceFunction 对列表进行聚合，再将聚合后的结果传递给 windowFunction，reduce、sum、max 等操作都会使用 ReduceApplyWindowFunction

	```java
	public class ReduceApplyWindowFunction<K, W extends Window, T, R>
		extends WrappingFunction<WindowFunction<T, R, K, W>>
		implements WindowFunction<T, R, K, W> {
	
		private static final long serialVersionUID = 1L;
	
		private final ReduceFunction<T> reduceFunction;
	
		public ReduceApplyWindowFunction(ReduceFunction<T> reduceFunction,
			WindowFunction<T, R, K, W> windowFunction) {
			super(windowFunction);
			this.reduceFunction = reduceFunction;
		}
	
		@Override
		public void apply(K k, W window, Iterable<T> input, Collector<R> out) throws Exception {
	
			T curr = null;
			for (T val: input) {
				if (curr == null) {
					curr = val;
				} else {
					curr = reduceFunction.reduce(curr, val);
				}
			}
			wrappedFunction.apply(k, window, Collections.singletonList(curr), out);
		}
	}	
	```
	
* ReduceApplyProcessWindowFunction

	ReduceApplyProcessWindowFunction 和 ReduceApplyWindowFunction 类似，但是 ReduceApplyProcessWindowFunction 内部有一个 InternalProcessApplyWindowContext 实例，可以用于访问上下文

	```java
	public class ReduceApplyProcessWindowFunction<K, W extends Window, T, R>
		extends ProcessWindowFunction<T, R, K, W> {
	
		private static final long serialVersionUID = 1L;
	
		private final ReduceFunction<T> reduceFunction;
		private final ProcessWindowFunction<T, R, K, W> windowFunction;
		private transient InternalProcessApplyWindowContext<T, R, K, W> ctx;
	
		public ReduceApplyProcessWindowFunction(ReduceFunction<T> reduceFunction, ProcessWindowFunction<T, R, K, W> windowFunction) {
			this.windowFunction = windowFunction;
			this.reduceFunction = reduceFunction;
		}
	
		@Override
		public void process(K k, final Context context, Iterable<T> input, Collector<R> out) throws Exception {
	
			T curr = null;
			for (T val: input) {
				if (curr == null) {
					curr = val;
				} else {
					curr = reduceFunction.reduce(curr, val);
				}
			}
	
			this.ctx.window = context.window();
			this.ctx.context = context;
			windowFunction.process(k, ctx, Collections.singletonList(curr), out);
		}
	
		@Override
		public void clear(final Context context) throws Exception {
			this.ctx.window = context.window();
			this.ctx.context = context;
			windowFunction.clear(ctx);
		}
	
		@Override
		public void open(Configuration configuration) throws Exception {
			FunctionUtils.openFunction(this.windowFunction, configuration);
			ctx = new InternalProcessApplyWindowContext<>(windowFunction);
		}
	
		@Override
		public void close() throws Exception {
			FunctionUtils.closeFunction(this.windowFunction);
		}
	
		@Override
		public void setRuntimeContext(RuntimeContext t) {
			super.setRuntimeContext(t);
	
			FunctionUtils.setFunctionRuntimeContext(this.windowFunction, t);
		}
	}
	```
	
* AggregateApplyWindowFunction

	AggregateApplyWindowFunction 与 ReduceApplyWindowFunction 类似，ReduceApplyWindowFunction 包裹的是 ReduceFunction，AggregateApplyWindowFunction 包裹的是 AggregateFunction，当 apply 方法被调用的时候，先使用 AggregateFunction 对列表进行聚合，再将结果传递给 windowFunction

	```java
	public class AggregateApplyWindowFunction<K, W extends Window, T, ACC, V, R>
		extends WrappingFunction<WindowFunction<V, R, K, W>>
		implements WindowFunction<T, R, K, W> {
	
		private static final long serialVersionUID = 1L;
	
		private final AggregateFunction<T, ACC, V> aggFunction;
	
		public AggregateApplyWindowFunction(AggregateFunction<T, ACC, V> aggFunction, WindowFunction<V, R, K, W> windowFunction) {
			super(windowFunction);
			this.aggFunction = aggFunction;
		}
	
		@Override
		public void apply(K key, W window, Iterable<T> values, Collector<R> out) throws Exception {
			ACC acc = aggFunction.createAccumulator();
	
			for (T val : values) {
				acc = aggFunction.add(val, acc);
			}
	
			wrappedFunction.apply(key, window, Collections.singletonList(aggFunction.getResult(acc)), out);
		}
	}
	```
	
## 总结

今天，我们介绍了窗口函数，在 WindowOperator 执行 emitWindowContents 的时候，会调用内部函数的 process 方法，在 process 方法中，如果外部函数是 WindowFunction，会调用 apply 方法，如果是 ProcessWindowFunction，会调用 process 方法，apply/process 方法计算后会将结果 emit 给下游