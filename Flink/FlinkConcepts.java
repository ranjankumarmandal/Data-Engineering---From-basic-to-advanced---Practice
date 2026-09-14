import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.*;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class FlinkConcepts {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(10)));
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.minutes(5), Time.seconds(10)));

        DataStreamSource<String> source = env.fromElements("hello", "world", "flink", "streaming");
        DataStreamSource<Integer> intSource = env.fromElements(1, 2, 3, 4, 5);
        DataStreamSource<Tuple2<String, Integer>> tupleSource = env.fromElements(
                new Tuple2<>("a", 1),
                new Tuple2<>("b", 2),
                new Tuple2<>("a", 3)
        );
        DataStreamSource<Tuple3<String, Integer, Double>> tuple3Source = env.fromElements(
                new Tuple3<>("a", 1, 1.0),
                new Tuple3<>("b", 2, 2.0),
                new Tuple3<>("a", 3, 3.0)
        );

        source.print();
        intSource.print();
        tupleSource.print();

        DataStream<String> mapped = source.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value.toUpperCase();
            }
        });

        DataStream<Integer> intMapped = intSource.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer value) throws Exception {
                return value * 2;
            }
        });

        DataStream<String> flatMapped = source.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                for (String s : value.split("")) {
                    out.collect(s);
                }
            }
        });

        DataStream<String> filtered = source.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return value.length() > 4;
            }
        });

        DataStream<Integer> keyByStream = tupleSource.keyBy(0).map(new MapFunction<Tuple2<String, Integer>, Integer>() {
            @Override
            public Integer map(Tuple2<String, Integer> value) throws Exception {
                return value.f1;
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> reduced = tupleSource.keyBy(0)
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                        return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
                    }
                });

        SingleOutputStreamOperator<Tuple2<String, Integer>> aggregated = tupleSource.keyBy(0)
                .fold(0, new FoldFunction<Tuple2<String, Integer>, Integer>() {
                    @Override
                    public Integer fold(Integer accumulator, Tuple2<String, Integer> value) throws Exception {
                        return accumulator + value.f1;
                    }
                }).map(new MapFunction<Integer, Tuple2<String, Integer>>() {
                    @Override
            public Tuple2<String, Integer> map(Integer value) throws Exception {
                return new Tuple2<>("sum", value);
            }
        });

        DataStream<Tuple2<String, Integer>> windowed = tupleSource
                .keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .sum(1);

        DataStream<Tuple2<String, Integer>> slidingWindowed = tupleSource
                .keyBy(0)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .sum(1);

        DataStream<Tuple2<String, Integer>> sessionWindowed = tupleSource
                .keyBy(0)
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(30)))
                .sum(1);

        DataStream<Tuple2<String, Integer>> globalWindowed = tupleSource
                .keyBy(0)
                .window(GlobalWindows.create())
                .trigger(ProcessingTimeTrigger.create())
                .evictor(TimeEvictor.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, GlobalWindow>() {
                    @Override
                    public void process(String key, Context ctx, Iterable<Tuple2<String, Integer>> values, Collector<Tuple2<String, Integer>> out) throws Exception {
                        int sum = 0;
                        for (Tuple2<String, Integer> value : values) {
                            sum += value.f1;
                        }
                        out.collect(new Tuple2<>(key, sum));
                    }
                });

        DataStream<Tuple2<String, Integer>> timeWindowed = tupleSource
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .aggregate(new AggregateFunction<Tuple2<String, Integer>, Integer, Integer>() {
                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    @Override
                    public Integer add(Tuple2<String, Integer> value, Integer accumulator) {
                        return accumulator + value.f1;
                    }

                    @Override
                    public Integer getResult(Integer accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Integer merge(Integer a, Integer b) {
                        return a + b;
                    }
                }).map(new MapFunction<Integer, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(Integer value) throws Exception {
                        return new Tuple2<>("sum", value);
                    }
                });

        DataStream<Tuple2<String, Integer>> processed = tupleSource
                .keyBy(0)
                .process(new KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
                    private ValueState<Integer> state;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        state = getRuntimeContext().getState(new ValueStateDescriptor<>("sum", Integer.class));
                    }

                    @Override
                    public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                        Integer currentSum = state.value();
                        if (currentSum == null) {
                            currentSum = 0;
                        }
                        currentSum += value.f1;
                        state.update(currentSum);
                        out.collect(new Tuple2<>(value.f0, currentSum));
                    }
                });

        OutputTag<String> lateOutputTag = new OutputTag<String>("late-data") {};

        SingleOutputStreamOperator<Tuple2<String, Integer>> withSideOutputs = tupleSource
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sideOutputLateData(lateOutputTag)
                .process(new ProcessWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context ctx, Iterable<Tuple2<String, Integer>> values, Collector<Tuple2<String, Integer>> out) throws Exception {
                        int sum = 0;
                        for (Tuple2<String, Integer> value : values) {
                            sum += value.f1;
                        }
                        out.collect(new Tuple2<>(key, sum));
                    }
                });

        DataStream<String> lateDataStream = withSideOutputs.getSideOutput(lateOutputTag);

        DataStreamSource<Long> timestampSource = env.addSource(new SourceFunction<Long>() {
            private volatile boolean isRunning = true;

            @Override
            public void run(SourceContext<Long> ctx) throws Exception {
                long count = 0;
                while (isRunning) {
                    ctx.collectWithTimestamp(count, System.currentTimeMillis());
                    ctx.emitWatermark(new Watermark(System.currentTimeMillis() - 1000));
                    count++;
                    Thread.sleep(100);
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        });

        DataStream<Tuple2<String, Integer>> joined = tupleSource
                .join(tupleSource)
                .where(0)
                .equalTo(0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .apply(new JoinFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> join(Tuple2<String, Integer> first, Tuple2<String, Integer> second) throws Exception {
                        return new Tuple2<>(first.f0, first.f1 + second.f1);
                    }
                });

        DataStream<Tuple2<String, Integer>> coGrouped = tupleSource
                .coGroup(tupleSource)
                .where(0)
                .equalTo(0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .apply(new CoGroupFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
                    @Override
                    public void coGroup(Iterable<Tuple2<String, Integer>> first, Iterable<Tuple2<String, Integer>> second, Collector<Tuple2<String, Integer>> out) throws Exception {
                        int sum = 0;
                        for (Tuple2<String, Integer> value : first) {
                            sum += value.f1;
                        }
                        for (Tuple2<String, Integer> value : second) {
                            sum += value.f1;
                        }
                        out.collect(new Tuple2<>("sum", sum));
                    }
                });

        DataStream<Tuple2<String, Integer>> connected = tupleSource
                .connect(intSource)
                .keyBy(0, 0)
                .process(new CoProcessFunction<Tuple2<String, Integer>, Integer, Tuple2<String, Integer>>() {
                    @Override
                    public void processElement1(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                        out.collect(value);
                    }

                    @Override
                    public void processElement2(Integer value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                        out.collect(new Tuple2<>("int", value));
                    }
                });

        DataStream<Tuple2<String, Integer>> unioned = tupleSource.union(tupleSource);

        DataStream<Tuple2<String, Integer>> splitStream = tupleSource
                .split(new OutputSelector<Tuple2<String, Integer>>() {
                    @Override
                    public Iterable<String> select(Tuple2<String, Integer> value) {
                        List<String> output = new ArrayList<>();
                        if (value.f1 > 2) {
                            output.add("large");
                        } else {
                            output.add("small");
                        }
                        return output;
                    }
                });

        DataStream<Tuple2<String, Integer>> iterated = intSource.iterate()
                .map(new MapFunction<Integer, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(Integer value) throws Exception {
                        return new Tuple2<>("iter", value);
                    }
                })
                .closeWith(intSource.filter(new FilterFunction<Integer>() {
                    @Override
                    public boolean filter(Integer value) throws Exception {
                        return value < 10;
                    }
                }));

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test");

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                "test-topic",
                new SimpleStringSchema(),
                properties
        );

        DataStream<String> kafkaSource = env.addSource(kafkaConsumer);

        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(
                "output-topic",
                new SimpleStringSchema(),
                properties
        );

        kafkaSource.addSink(kafkaProducer);

        source.addSink(new SinkFunction<String>() {
            @Override
            public void invoke(String value, Context context) throws Exception {
                System.out.println("Sink: " + value);
            }
        });

        source.writeAsText("output.txt");

        tupleSource.writeAsCsv("output.csv");

        source.writeToSocket("localhost", 9999, new SimpleStringSchema());

        DataStream<Tuple2<String, Integer>> withState = tupleSource
                .keyBy(0)
                .process(new KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
                    private ValueState<Integer> countState;
                    private ListState<Integer> listState;
                    private MapState<String, Integer> mapState;
                    private ReducingState<Integer> reducingState;
                    private AggregatingState<Integer, Integer> aggregatingState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        countState = getRuntimeContext().getState(new ValueStateDescriptor<>("count", Integer.class));
                        listState = getRuntimeContext().getListState(new ListStateDescriptor<>("list", Integer.class));
                        mapState = getRuntimeContext().getMapState(new MapStateDescriptor<>("map", String.class, Integer.class));
                        reducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<>("reducing", new ReduceFunction<Integer>() {
                            @Override
                            public Integer reduce(Integer value1, Integer value2) throws Exception {
                                return value1 + value2;
                            }
                        }, Integer.class));
                        aggregatingState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Integer, Integer, Integer>("aggregating", new AggregateFunction<Integer, Integer, Integer>() {
                            @Override
                            public Integer createAccumulator() {
                                return 0;
                            }

                            @Override
                            public Integer add(Integer value, Integer accumulator) {
                                return accumulator + value;
                            }

                            @Override
                            public Integer getResult(Integer accumulator) {
                                return accumulator;
                            }

                            @Override
                            public Integer merge(Integer a, Integer b) {
                                return a + b;
                            }
                        }, Integer.class));
                    }

                    @Override
                    public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                        Integer count = countState.value();
                        if (count == null) {
                            count = 0;
                        }
                        count++;
                        countState.update(count);
                        listState.add(value.f1);
                        mapState.put(value.f0, value.f1);
                        reducingState.add(value.f1);
                        aggregatingState.add(value.f1);
                        out.collect(new Tuple2<>(value.f0, count));
                    }
                });

        DataStream<Tuple2<String, Integer>> timerBased = tupleSource
                .keyBy(0)
                .process(new KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
                    @Override
                    public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                        ctx.timerService().registerProcessingTimeTimer(System.currentTimeMillis() + 5000);
                        out.collect(value);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                        out.collect(new Tuple2<>("timer", (int) timestamp));
                    }
                });

        DataStream<Tuple2<String, Integer>> asyncOperation = tupleSource
                .keyBy(0)
                .process(new KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
                    @Override
                    public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                        CompletableFuture.supplyAsync(() -> {
                            try {
                                Thread.sleep(100);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                            return value.f1 * 2;
                        }).thenAccept(result -> {
                            out.collect(new Tuple2<>(value.f0, result));
                        });
                    }
                });

        DataStream<Tuple2<String, Integer>> partitioned = tupleSource
                .partitionCustom(new Partitioner<String>() {
                    @Override
                    public int partition(String key, int numPartitions) {
                        return key.hashCode() % numPartitions;
                    }
                }, 0);

        DataStream<Tuple2<String, Integer>> shuffled = tupleSource.shuffle();

        DataStream<Tuple2<String, Integer>> rebalanced = tupleSource.rebalance();

        DataStream<Tuple2<String, Integer>> rescaled = tupleSource.rescale();

        DataStream<Tuple2<String, Integer>> broadcast = tupleSource.broadcast();

        DataStream<Tuple2<String, Integer>> forward = tupleSource.forward();

        DataStream<Tuple2<String, Integer>> keyPartitioned = tupleSource.keyBy(0);

        DataStream<Tuple2<String, Integer>> global = tupleSource.global();

        DataStream<Tuple2<String, Integer>> withInterval = tupleSource
                .keyBy(0)
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))
                .process(new ProcessWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context ctx, Iterable<Tuple2<String, Integer>> values, Collector<Tuple2<String, Integer>> out) throws Exception {
                        int sum = 0;
                        for (Tuple2<String, Integer> value : values) {
                            sum += value.f1;
                        }
                        out.collect(new Tuple2<>(key, sum));
                    }
                });

        DataStream<Tuple2<String, Integer>> dynamicWindowed = tupleSource
                .keyBy(0)
                .window(DynamicEventTimeWindows.of(new WindowAssigner.WindowAssignerContext() {
                    @Override
                    public long getCurrentProcessingTime() {
                        return System.currentTimeMillis();
                    }
                }, new WindowAssignerTrigger() {
                    @Override
                    public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
                        return TriggerResult.CONTINUE;
                    }

                    @Override
                    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                        return TriggerResult.CONTINUE;
                    }

                    @Override
                    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                        return TriggerResult.CONTINUE;
                    }

                    @Override
                    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
                    }
                }));

        DataStream<Tuple2<String, Integer>> countWindowed = tupleSource
                .keyBy(0)
                .countWindow(5)
                .sum(1);

        DataStream<Tuple2<String, Integer>> slidingCountWindowed = tupleSource
                .keyBy(0)
                .countWindow(5, 2)
                .sum(1);

        DataStream<Tuple2<String, Integer>> customTrigger = tupleSource
                .keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .trigger(new Trigger<Tuple2<String, Integer>, TimeWindow>() {
                    @Override
                    public TriggerResult onElement(Tuple2<String, Integer> element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
                        return TriggerResult.CONTINUE;
                    }

                    @Override
                    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                        return TriggerResult.FIRE_AND_PURGE;
                    }

                    @Override
                    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                        return TriggerResult.CONTINUE;
                    }

                    @Override
                    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
                    }
                })
                .process(new ProcessWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context ctx, Iterable<Tuple2<String, Integer>> values, Collector<Tuple2<String, Integer>> out) throws Exception {
                        int sum = 0;
                        for (Tuple2<String, Integer> value : values) {
                            sum += value.f1;
                        }
                        out.collect(new Tuple2<>(key, sum));
                    }
                });

        DataStream<Tuple2<String, Integer>> customEvictor = tupleSource
                .keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .evictor(new Evictor<Tuple2<String, Integer>, TimeWindow>() {
                    @Override
                    public void evictBefore(Iterable<TimestampedValue<Tuple2<String, Integer>>> elements, int size, TimeWindow window, EvictorContext ctx) {
                    }

                    @Override
                    public void evictAfter(Iterable<TimestampedValue<Tuple2<String, Integer>>> elements, int size, TimeWindow window, EvictorContext ctx) {
                    }
                })
                .process(new ProcessWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context ctx, Iterable<Tuple2<String, Integer>> values, Collector<Tuple2<String, Integer>> out) throws Exception {
                        int sum = 0;
                        for (Tuple2<String, Integer> value : values) {
                            sum += value.f1;
                        }
                        out.collect(new Tuple2<>(key, sum));
                    }
                });

        DataStream<Tuple2<String, Integer>> withAllowedLateness = tupleSource
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .allowedLateness(Time.seconds(2))
                .sideOutputLateData(lateOutputTag)
                .sum(1);

        DataStream<Tuple2<String, Integer>> minByWindow = tupleSource
                .keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .minBy(1);

        DataStream<Tuple2<String, Integer>> maxByWindow = tupleSource
                .keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .maxBy(1);

        DataStream<Tuple2<String, Integer>> minWindow = tupleSource
                .keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .min(1);

        DataStream<Tuple2<String, Integer>> maxWindow = tupleSource
                .keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .max(1);

        DataStream<Tuple2<String, Integer>> distinctWindow = tupleSource
                .keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context ctx, Iterable<Tuple2<String, Integer>> values, Collector<Tuple2<String, Integer>> out) throws Exception {
                        Set<Integer> distinctValues = new HashSet<>();
                        for (Tuple2<String, Integer> value : values) {
                            distinctValues.add(value.f1);
                        }
                        for (Integer val : distinctValues) {
                            out.collect(new Tuple2<>(key, val));
                        }
                    }

                });

        env.execute("Flink Concepts");
    }
}
