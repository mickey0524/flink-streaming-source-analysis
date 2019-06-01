# flink 中的异步操作符

前面我们介绍的所有操作符都是同步的，当流元素到达操作符，执行 ProcessElement 方法，将处理的结果 emit 到下一个操作符，这篇文章我们来讲一下 flink 中的异步操作符

我们先简要介绍一下 flink 是如何异步操作流元素的，当流元素到达异步操作符，我们将流元素暂时存储到一个队列中，当用户针对这个流元素执行完某种异步操作，得到结果之后，执行 asyncInvoke 方法通知异步操作符，异步操作符再将结果 emit 给下游操作符

## 一个小栗子

```java
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
Integer[] integers = new Integer[]{1, 2, 3, 4};

DataStream<Integer> dataStream = env.fromElements(integers);

AsyncDataStream.orderedWait(dataStream, new AsyncFunction<Integer, Integer>() {
    @Override
    public void asyncInvoke(Integer input, ResultFuture<Integer> resultFuture) throws Exception {
        Thread.sleep(ThreadLocalRandom.current().nextInt(5));
        ArrayList<Integer> l = new ArrayList<>();
        l.add(input);
        resultFuture.complete(l);
    }
}, 10, TimeUnit.MILLISECONDS).printToErr();
```

👆的栗子非常简单，从本地集合生成流，然后调用 AsyncDataStream 的 ordererWait 方法接收 AsyncFunction，进而生成 AsyncWaitOperator，我们在 asyncInvoke 中随机等待一段时间，然后通知异步操作符操作完成，可以将元素输出到错误流了

我们一口气说了很多名词，很多童鞋可能一头雾水，没有关系，我们下面一个一个类来讲解

## AsyncDataStream

AsyncDataStream 其实可以说是一个工具类，用于给 DataStream 加上异步操作符，AsyncDataStream 提供两个方法 —— orderedWait 和 unorderedWait，orderedWait 能够让流元素流出操作符的顺序和流入操作符的顺序完全相同，unorderedWait 保证 Watermark 和 StreamRecord 的相对顺序一致，但是两个 Watermark 之间的 StreamRecord 可以乱序输出

```java
public class AsyncDataStream {
	private static <IN, OUT> SingleOutputStreamOperator<OUT> addOperator(
			DataStream<IN> in,
			AsyncFunction<IN, OUT> func,
			long timeout,
			int bufSize,
			OutputMode mode) {

		AsyncWaitOperator<IN, OUT> operator = new AsyncWaitOperator<>(
			in.getExecutionEnvironment().clean(func),
			timeout,
			bufSize,
			mode);
		
		return in.transform("async wait operator", outTypeInfo, operator);
	}

	public static <IN, OUT> SingleOutputStreamOperator<OUT> unorderedWait(
			DataStream<IN> in,
			AsyncFunction<IN, OUT> func,
			long timeout,
			TimeUnit timeUnit,
			int capacity) {
		return addOperator(in, func, timeUnit.toMillis(timeout), capacity, OutputMode.UNORDERED);
	}

	public static <IN, OUT> SingleOutputStreamOperator<OUT> orderedWait(
			DataStream<IN> in,
			AsyncFunction<IN, OUT> func,
			long timeout,
			TimeUnit timeUnit,
			int capacity) {
		return addOperator(in, func, timeUnit.toMillis(timeout), capacity, OutputMode.ORDERED);
	}
}
```

从源码中可以看到，orderedWait 方法和 unorderedWait 方法都调用了 addOperator 方法，addOperator 用于生成 AsyncWaitOperator，然后调用输入流的 transform 方法生成 StreamTransformation 并加入 Transformation 树用于生成 StreamGraph

我们再来看一下 orderedWait 和 unorderedWait 的参数

* DataStream<IN> in: 输入流，也就是添加异步操作符的目标流
* AsyncFunction<IN, OUT> func: 用户定义的异步函数，主要用于异步操作完毕后通知操作符，以及在异步操作超时的时候抛出异常
* long timeout: 异步操作处理时间的 ttl
* TimeUnit timeUnit: 异步操作处理时间的单位
* int capacity: 缓存流元素的队列的大小，默认为 100

## AsyncFunction

👇是 AsyncFunction 的代码，可以看到 AsyncFunction 接口定义了两个函数，asyncInvoke 函数用于在异步操作完毕的时候通知操作符（例如访问 MySQL 完成了），调用 ResultFuture 的 complete 即可，timeout 函数用于 asyncInvoke 函数执行时间超过 orderedWait 和 unorderedWait 中
timeout 参数的时候抛出异常

```java
public interface AsyncFunction<IN, OUT> extends Function, Serializable {
	/**
	 * 触发每个流输入的异步操作
	 */
	void asyncInvoke(IN input, ResultFuture<OUT> resultFuture) throws Exception;

	/**
	 * asyncInvoke 操作 timeout 了，默认抛出异常
	 */
	default void timeout(IN input, ResultFuture<OUT> resultFuture) throws Exception {
		resultFuture.completeExceptionally(
			new TimeoutException("Async function call has timed out."));
	}
}
```

## 🚧 Under Construction

## OrderedStreamElementQueue

## UnorderedStreamElementQueue

## StreamElementQueueEntry

## Emitter

## AsyncWaitOperator