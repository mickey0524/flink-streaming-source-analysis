# flink DataStream 依托窗口完成的操作（coGroup、join）

前面四篇文章，我们从不同的方面介绍了 flink 的窗口操作，相信大家对窗口操作已经有了一定的基础，这篇文章，我们来介绍一下 flink 中依托窗口完成的操作 —— coGroup 和 join

## coGroup

coGroup 是 DataStream 中的 API，用来 join 两个 DataStream，生成一个 CoGroupedStreams

```java
public <T2> CoGroupedStreams<T, T2> coGroup(DataStream<T2> otherStream) {
	return new CoGroupedStreams<>(this, otherStream);
}
```

### CoGroupedStreams

首先来看一下 `CoGroupedStreams.java` 中官方给出的 Example：

```java
DataStream<Tuple2<String, Integer>> one = ...;
DataStream<Tuple2<String, Integer>> two = ...;

DataStream<T> result = one.coGroup(two)
    .where(new MyFirstKeySelector())
    .equalTo(new MyFirstKeySelector())
    .window(TumblingEventTimeWindows.of(Time.of(5, TimeUnit.SECONDS)))
    .apply(new MyCoGroupFunction());
}
```

由上易得，coGroup 操作会将两个输入流合并成一个 CoGroupedStreams，where 操作接收一个 KeySelector，服务于第一个数据流，equalTo 操作接收另外一个 KeySelector，服务于第二个数据流，其实可以这么理解，假设第一个数据流元素执行 KeySelector 得到的 key 为 key1，第二个数据流元素执行 KeySelector 得到的 key 为 key2，CoGroupedStreams 会将 key1 和 key2 相等的两个流的元素放到一个窗口中，apply 操作会调用 WindowedStream 的 apply 方法，当窗口出发的时候，执行 MyCoGroupFunction 方法

* 构造函数

	CoGroupedStreams 的构造函数较简单，就定义了 input1 和 input2 两个 DataStream

	```java
	private final DataStream<T1> input1;  // 第一个 input

	private final DataStream<T2> input2;  // 第二个 input

	/**
	 * 创建新的 CoGroupedStreams
	 */
	public CoGroupedStreams(DataStream<T1> input1, DataStream<T2> input2) {
		this.input1 = requireNonNull(input1);
		this.input2 = requireNonNull(input2);
	}
	```

* where 方法
	
	where 方法会返回一个 Where 的内部类，Where类 包含第一个输入流的 KeySelector 以及第一个 KeySelector 得到的 keyType，Where 类中提供 equalTo 方法，用来接收第二个输入流的 KeySelector
	
	```java
	public class Where<KEY> {

		private final KeySelector<T1, KEY> keySelector1;
		private final TypeInformation<KEY> keyType;

		Where(KeySelector<T1, KEY> keySelector1, TypeInformation<KEY> keyType) {
			this.keySelector1 = keySelector1;
			this.keyType = keyType;
		}

		/**
		 * 为第二个 input 定义 KeySelector
		 */
		public EqualTo equalTo(KeySelector<T2, KEY> keySelector)  {
			Preconditions.checkNotNull(keySelector);
			final TypeInformation<KEY> otherKey = TypeExtractor.getKeySelectorTypes(keySelector, input2.getType());
			return equalTo(keySelector, otherKey);
		}

		public EqualTo equalTo(KeySelector<T2, KEY> keySelector, TypeInformation<KEY> keyType)  {
			Preconditions.checkNotNull(keySelector);
			Preconditions.checkNotNull(keyType);

			// 执行 coGroup 操作的两个 key 类型必须是一样的
			if (!keyType.equals(this.keyType)) {
				throw new IllegalArgumentException("The keys for the two inputs are not equal: " +
						"first key = " + this.keyType + " , second key = " + keyType);
			}

			return new EqualTo(input2.clean(keySelector));
		}

		/**
		 * 为两个 input 都定义好了 KeySelector 的 co-group 操作
		 */
		@Public
		public class EqualTo {

			private final KeySelector<T2, KEY> keySelector2;

			EqualTo(KeySelector<T2, KEY> keySelector2) {
				this.keySelector2 = requireNonNull(keySelector2);
			}

			/**
			 * 定义 co-group 操作的窗口
			 */
			@PublicEvolving
			public <W extends Window> WithWindow<T1, T2, KEY, W> window(WindowAssigner<? super TaggedUnion<T1, T2>, W> assigner) {
				return new WithWindow<>(input1, input2, keySelector1, keySelector2, keyType, assigner, null, null, null);
			}
		}
	}
	```

* window 方法

	在定义两个输入流的 KeySelector 之后，可以执行 window 方法，生成一个 WithWindow 实例，WithWindow 类内部包含 WindowAssigner、Trigger、Evictor、allowedLateness 等创建窗口所需的字段
 	
	```java
	private final DataStream<T1> input1;  // 第一个 input
	private final DataStream<T2> input2;  // 第二个 input

	private final KeySelector<T1, KEY> keySelector1;  // 第一个 input 的 KeySelector
	private final KeySelector<T2, KEY> keySelector2;  // 第二个 input 的 KeySelector

	private final TypeInformation<KEY> keyType;  // 做 coGroup 的 key 的类型

	private final WindowAssigner<? super TaggedUnion<T1, T2>, W> windowAssigner;  // WindowAssigner 给元素分配窗口

	private final Trigger<? super TaggedUnion<T1, T2>, ? super W> trigger;  // 触发器

	private final Evictor<? super TaggedUnion<T1, T2>, ? super W> evictor;  // 触发器

	private final Time allowedLateness;  // 窗口允许的延迟

	private WindowedStream<TaggedUnion<T1, T2>, KEY, W> windowedStream;  // 窗口流
	```
	
	WithWindow 类的 apply 方法会创建一个 WindowedStream，并调用 WindowedStream 的 apply 方法
	
	在介绍 apply 之前，我们需要介绍如下几个类
	
	* UnionKeySelector

		UnionKeySelector 实现了 KeySelector 接口，会包裹两个输入流的 KeySelector，当调用 getKey 方法的时候，如果流元素来自第一个输入流，会调用 keySelector1，反之，调用 keySelector2
	
		```java
		private static class UnionKeySelector<T1, T2, KEY> implements KeySelector<TaggedUnion<T1, T2>, KEY> {
			private static final long serialVersionUID = 1L;
	
			private final KeySelector<T1, KEY> keySelector1;
			private final KeySelector<T2, KEY> keySelector2;
	
			public UnionKeySelector(KeySelector<T1, KEY> keySelector1,
					KeySelector<T2, KEY> keySelector2) {
				this.keySelector1 = keySelector1;
				this.keySelector2 = keySelector2;
			}
	
			@Override
			public KEY getKey(TaggedUnion<T1, T2> value) throws Exception{
				if (value.isOne()) {
					return keySelector1.getKey(value.getOne());
				} else {
					return keySelector2.getKey(value.getTwo());
				}
			}
		}
		```
	
	* TaggedUnion

		TaggedUnion 是窗口流的元素类型，一个 TaggedUnion 包裹一个来自 input1 或 input2 的元素，当 one 为 null 的时候，元素来自 input2，当 two 为 null 的时候，元素来自 input1

		```java
		public static class TaggedUnion<T1, T2> {
			private final T1 one;
			private final T2 two;
	
			private TaggedUnion(T1 one, T2 two) {
				this.one = one;
				this.two = two;
			}
	
			public boolean isOne() {
				return one != null;
			}
	
			public boolean isTwo() {
				return two != null;
			}
	
			public T1 getOne() {
				return one;
			}
	
			public T2 getTwo() {
				return two;
			}
	
			public static <T1, T2> TaggedUnion<T1, T2> one(T1 one) {
				return new TaggedUnion<>(one, null);
			}
	
			public static <T1, T2> TaggedUnion<T1, T2> two(T2 two) {
				return new TaggedUnion<>(null, two);
			}
		}
		```
	
	* Input1Tagger && Input2Tagger

		Input1Tagger 和 Input2Tagger 用来对 input1 和 input2 做 map 操作，使得 input1 和 input2 的类型都变为 TaggedUnion

		```java
		private static class Input1Tagger<T1, T2> implements MapFunction<T1, TaggedUnion<T1, T2>> {
			private static final long serialVersionUID = 1L;
	
			@Override
			public TaggedUnion<T1, T2> map(T1 value) throws Exception {
				return TaggedUnion.one(value);
			}
		}
	
		// 从第二个 input 中获取 TaggedUnion
		private static class Input2Tagger<T1, T2> implements MapFunction<T2, TaggedUnion<T1, T2>> {
			private static final long serialVersionUID = 1L;
	
			@Override
			public TaggedUnion<T1, T2> map(T2 value) throws Exception {
				return TaggedUnion.two(value);
			}
		}
		```
	
	* CoGroupWindowFunction

		CoGroupWindowFunction 方法继承 WindowFunction，内部包裹一个 CoGroupFunction，当窗口触发的时候，会将窗口中的元素分为两个 ArrayList，ArrayList1 中都是 input1 中的元素，ArrayList2 中都是 input2 中的元素，然后调用 CoGroupFunction
		
		```java
		private static class CoGroupWindowFunction<T1, T2, T, KEY, W extends Window>
				extends WrappingFunction<CoGroupFunction<T1, T2, T>>
				implements WindowFunction<TaggedUnion<T1, T2>, T, KEY, W> {
	
			private static final long serialVersionUID = 1L;
	
			public CoGroupWindowFunction(CoGroupFunction<T1, T2, T> userFunction) {
				super(userFunction);
			}
	
			@Override
			public void apply(KEY key,
					W window,
					Iterable<TaggedUnion<T1, T2>> values,
					Collector<T> out) throws Exception {
	
				List<T1> oneValues = new ArrayList<>();
				List<T2> twoValues = new ArrayList<>();
	
				for (TaggedUnion<T1, T2> val: values) {
					if (val.isOne()) {
						oneValues.add(val.getOne());
					} else {
						twoValues.add(val.getTwo());
					}
				}
				wrappedFunction.coGroup(oneValues, twoValues, out);
			}
		}
		```
	
	* CoGroupFunction

		CoGroupFunction 是用户提供的函数，内置一个 coGroup 函数，first 是 input1 元素的集合，second 是 input2 元素的集合

		```java
		public interface CoGroupFunction<IN1, IN2, O> extends Function, Serializable {
			void coGroup(Iterable<IN1> first, Iterable<IN2> second, Collector<O> out) throws Exception;
		}
		```
	
	了解了上述类之后，我们再来看看 apply 方法
	
	apply 方法会对 input1/input2 方法执行 map 函数，参数为 Input1Tagger/Input2Tagger，将 input1/input2 的元素类型设为 TaggedUnion，然后执行 union 方法将 input1 和 input2 合并为一个 DataStream，并由这个 DataStream 生成一个窗口流，最后传入 CoGroupWindowFunction 函数，调用窗口流的 apply 方法完成窗口操作
	
	```java
	public <T> DataStream<T> apply(CoGroupFunction<T1, T2, T> function, TypeInformation<T> resultType) {

		UnionTypeInfo<T1, T2> unionType = new UnionTypeInfo<>(input1.getType(), input2.getType());
		UnionKeySelector<T1, T2, KEY> unionKeySelector = new UnionKeySelector<>(keySelector1, keySelector2);

		DataStream<TaggedUnion<T1, T2>> taggedInput1 = input1
				.map(new Input1Tagger<T1, T2>())
				.setParallelism(input1.getParallelism())
				.returns(unionType);
		DataStream<TaggedUnion<T1, T2>> taggedInput2 = input2
				.map(new Input2Tagger<T1, T2>())
				.setParallelism(input2.getParallelism())
				.returns(unionType);

		DataStream<TaggedUnion<T1, T2>> unionStream = taggedInput1.union(taggedInput2);

		windowedStream =
				new KeyedStream<TaggedUnion<T1, T2>, KEY>(unionStream, unionKeySelector, keyType)
				.window(windowAssigner);

		if (trigger != null) {
			windowedStream.trigger(trigger);
		}
		if (evictor != null) {
			windowedStream.evictor(evictor);
		}
		if (allowedLateness != null) {
			windowedStream.allowedLateness(allowedLateness);
		}

		return windowedStream.apply(new CoGroupWindowFunction<T1, T2, T, KEY, W>(function), resultType);
	}
	```

### 小栗子

```java
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

String host = "127.0.0.1";
int port = 9000;

  Integer[] integers1 = new Integer[]{1, 2, 3, 4, 5, 6};

  DataStream<Integer> dataStream1 = env.fromElements(integers1);
  DataStream<Integer> dataStream2 = env.socketTextStream(host, port).map(new MapFunction<String, Integer>() {
      @Override
      public Integer map(String value) throws Exception {
          return Integer.valueOf(value);
      }
  });
  
  dataStream1.coGroup(dataStream2).where(new KeySelector<Integer, Integer>() {
      @Override
      public Integer getKey(Integer value) throws Exception {
          return 0;
      }
  }).equalTo(new KeySelector<Integer, Integer>() {
      @Override
      public Integer getKey(Integer value) throws Exception {
          return 0;
      }
  }).window(TumblingProcessingTimeWindows.of(Time.of(5, TimeUnit.SECONDS))).apply(new CoGroupFunction<Integer, Integer, String>() {
      @Override
      public void coGroup(Iterable<Integer> first, Iterable<Integer> second, Collector<String> out) throws Exception {
          for (Integer m : first) {
              for (Integer n : second) {
                  out.collect("m: " + m + " n: " + n + " = " + (m + n));
              }
          }
      }
  }).printToErr();
```

## join

join 是 DataStream 中的 API，同样用来 join 两个 DataStream，生成一个 JoinedStreams

```java
public <T2> JoinedStreams<T, T2> join(DataStream<T2> otherStream) {
	return new JoinedStreams<>(this, otherStream);
}
```

JoinedStreams 和 CoGroupedStreams 非常相似（JoinedStreams 的 WithWindow 类调用 CoGroupedStreams 的 WithWindow），这里就不详细介绍了，感兴趣的同学可以自行去看看，这里我们来讲将不同之处

JoinedStreams 中 WithWindow 的 apply 方法接收的是 FlatJoinFunction 或 JoinFunction，这两个接口定义了 join 方法，JoinFunction.join 接收两个流元素，返回一个流元素，FlatJoinFunction 还接收一个 Collector，能够输出零个或多个元素

```java
public interface FlatJoinFunction<IN1, IN2, OUT> extends Function, Serializable {
	void join(IN1 first, IN2 second, Collector<OUT> out) throws Exception;
}

public interface JoinFunction<IN1, IN2, OUT> extends Function, Serializable {
	OUT join(IN1 first, IN2 second) throws Exception;
}
```

我们知道 CoGroupedStreams.WithWindow.apply 接收 CoGroupFunction，因此 JoinedStreams 定义了 FlatJoinCoGroupFunction 和 JoinCoGroupFunction，这两个类实现了 CoGroupFunction，内部包裹了 FlatJoinFunction/JoinFunction

```java
private static class FlatJoinCoGroupFunction<T1, T2, T>
		extends WrappingFunction<FlatJoinFunction<T1, T2, T>>
		implements CoGroupFunction<T1, T2, T> {
	private static final long serialVersionUID = 1L;

	public FlatJoinCoGroupFunction(FlatJoinFunction<T1, T2, T> wrappedFunction) {
		super(wrappedFunction);
	}

	@Override
	public void coGroup(Iterable<T1> first, Iterable<T2> second, Collector<T> out) throws Exception {
		for (T1 val1: first) {
			for (T2 val2: second) {
				wrappedFunction.join(val1, val2, out);
			}
		}
	}
}

private static class JoinCoGroupFunction<T1, T2, T>
		extends WrappingFunction<JoinFunction<T1, T2, T>>
		implements CoGroupFunction<T1, T2, T> {
	private static final long serialVersionUID = 1L;

	public JoinCoGroupFunction(JoinFunction<T1, T2, T> wrappedFunction) {
		super(wrappedFunction);
	}

	@Override
	public void coGroup(Iterable<T1> first, Iterable<T2> second, Collector<T> out) throws Exception {
		for (T1 val1: first) {
			for (T2 val2: second) {
				out.collect(wrappedFunction.join(val1, val2));
			}
		}
	}
}
```

因此，JoinedStreams 的 apply 操作的数据流动为 GoGroupStreams.CoGroupWindowFunction => JoinedStreams.JoinCoGroupFunction/JoinedStreams.FlatJoinCoGroupFunction => 用户定义的 FlatJoinFunction/JoinFunction

## 总结

今天我们讲解了一下 DataStream 中的两个 API —— coGroup 和 join，进而介绍了 CoGroupStreams 和 JoinedStreams，希望对大家有所帮助