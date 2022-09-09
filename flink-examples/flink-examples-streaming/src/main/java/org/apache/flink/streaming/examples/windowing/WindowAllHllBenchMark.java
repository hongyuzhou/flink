package org.apache.flink.streaming.examples.windowing;

import org.apache.datasketches.hll.HllSketch;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class WindowAllHllBenchMark {

    private static final Logger LOG = LoggerFactory.getLogger(WindowAllHllBenchMark.class);

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        env.enableCheckpointing(500, CheckpointingMode.EXACTLY_ONCE);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("file:///Users/didi/flink-checkpoint");

        DataStream<String> text = env
                .readTextFile("/Users/didi/ibt/code/flink-tpcds-data/all_table_1g/store_sales.dat");

        // Tuple3<String, Integer, Long>
        text
                .flatMap(new WindowHllBenchMark.Splitter())
                //.keyBy(value -> value.f0)
                .windowAll(TumblingProcessingTimeWindows.of(Time.milliseconds(1000)))
                .hll(2)
//                .aggregate(new AggregateFunction<Tuple3<String, Integer, Long>, Tuple4<HllSketch, Integer, List<Long>, List<String>>, HllSketch>() {
//
//                    @Override
//                    public Tuple4<HllSketch, Integer, List<Long>, List<String>> createAccumulator() {
//                        return Tuple4.of(
//                                new HllSketch(),
//                                0,
//                                new ArrayList<>(),
//                                new ArrayList<>());
//                    }
//
//                    @Override
//                    public Tuple4<HllSketch, Integer, List<Long>, List<String>> add(
//                            Tuple3<String, Integer, Long> value,
//                            Tuple4<HllSketch, Integer, List<Long>, List<String>> accumulator) {
//                        accumulator.f0.update(value.f2);
//                        accumulator.f1 += 1;
//                        accumulator.f2.add(value.f2);
//                        accumulator.f3.add(value.f0);
//                        return Tuple4.of(
//                                accumulator.f0,
//                                accumulator.f1,
//                                accumulator.f2,
//                                accumulator.f3);
//                    }
//
//                    @Override
//                    public HllSketch getResult(Tuple4<HllSketch, Integer, List<Long>, List<String>> accumulator) {
//
//                        LOG.info(
//                                "Count is: {}, Estimate is: {}, KeyList is: {}",
//                                accumulator.f1,
//                                accumulator.f0.getEstimate(),
//                                accumulator.f2.toString());
//                                //accumulator.f3.toString());
//                        return accumulator.f0;
//                    }
//
//                    @Override
//                    public Tuple4<HllSketch, Integer, List<Long>, List<String>> merge(
//                            Tuple4<HllSketch, Integer, List<Long>, List<String>> a,
//                            Tuple4<HllSketch, Integer, List<Long>, List<String>> b) {
//                        return null;
//                    }
//                })
                .print();
        env.execute();
    }
}
