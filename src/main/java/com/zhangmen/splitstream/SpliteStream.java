package com.zhangmen.splitstream;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import java.util.ArrayList;
import java.util.List;

/**
 * flink的分流器
 */
public class SpliteStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //获取数据源
        List data = new ArrayList<Tuple3<Integer,Integer,Integer>>();
        data.add(new Tuple3<>(0,1,0));
        data.add(new Tuple3<>(0,1,1));
        data.add(new Tuple3<>(0,2,2));
        data.add(new Tuple3<>(0,1,3));
        data.add(new Tuple3<>(1,2,5));
        data.add(new Tuple3<>(1,2,9));
        data.add(new Tuple3<>(1,2,11));
        data.add(new Tuple3<>(1,2,13));

        DataStreamSource<Tuple3<Integer,Integer,Integer>> items = env.fromCollection(data);

//        filterSpliteStream(items);
//        outSelector(items);
        recommendSpliteStream(items);
        env.execute("splite");
        Thread.sleep(100000L);
    }

    /**
     * 官方推荐使用分流器
     * @param items
     */
    private static void recommendSpliteStream(DataStreamSource<Tuple3<Integer, Integer, Integer>> items) {
        OutputTag<Tuple3<Integer,Integer,Integer>> zeroStream = new OutputTag<Tuple3<Integer,Integer,Integer>>("zeroStream") {};
        OutputTag<Tuple3<Integer,Integer,Integer>> oneStream = new OutputTag<Tuple3<Integer,Integer,Integer>>("oneStream") {};
        SingleOutputStreamOperator<Tuple3<Integer, Integer, Integer>> processStream= items.process(new ProcessFunction<Tuple3<Integer, Integer, Integer>, Tuple3<Integer, Integer, Integer>>() {
            @Override
            public void processElement(Tuple3<Integer, Integer, Integer> value, Context ctx, Collector<Tuple3<Integer, Integer, Integer>> out) throws Exception {

                if (value.f0 == 0) {
                    ctx.output(zeroStream, value);
                } else if (value.f0 == 1) {
                    ctx.output(oneStream, value);
                }
            }
        });
        DataStream<Tuple3<Integer, Integer, Integer>> zeroSideOutput = processStream.getSideOutput(zeroStream);
        DataStream<Tuple3<Integer, Integer, Integer>> oneSideOutput = processStream.getSideOutput(oneStream);
        zeroSideOutput.print();
        oneSideOutput.printToErr();
    }

    /**
     * 过时的分流器（不能二次分流）
     * @param items
     */
    private static void outSelector(DataStreamSource<Tuple3<Integer, Integer, Integer>> items) {
        SplitStream<Tuple3<Integer, Integer, Integer>> splitStream = items.split(new OutputSelector<Tuple3<Integer, Integer, Integer>>() {
            @Override
            public Iterable<String> select(Tuple3<Integer, Integer, Integer> value) {
                List<String> tags = new ArrayList<>();
                if (value.f0 == 0) {
                    tags.add("zeroStream");
                } else if (value.f0 == 1) {
                    tags.add("oneStream");
                }
                return tags;
            }
        });
        splitStream.select("zeroStream").print();
        splitStream.select("oneStream").printToErr();
    }
    /**
     *用filter来进行分流
     * @param items
     */
    private static void filterSpliteStream(DataStreamSource<Tuple3<Integer, Integer, Integer>> items) {
        SingleOutputStreamOperator<Tuple3<Integer, Integer, Integer>> zeroStream = items
                .filter((FilterFunction<Tuple3<Integer, Integer, Integer>>) value -> value.f0 == 0);
        SingleOutputStreamOperator<Tuple3<Integer, Integer, Integer>> oneStream = items
                .filter((FilterFunction<Tuple3<Integer, Integer, Integer>>) value -> value.f0 == 1);
        zeroStream.print();
        oneStream.printToErr();
    }
}
