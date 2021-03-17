package com.zhangmen.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.Collection;

public class WindowWordCount {

    public static void main(String[] args) throws Exception {

//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DateSet(env);
//        Stream(env);
    }

    private static void DateSet(ExecutionEnvironment env ) throws Exception {
        //第一步创建需要广播的数据集
        DataSet<Integer> toBroadcast = env.fromElements(1, 2, 3);
        DataSet<String> data = env.fromElements("a", "b");

        MapOperator<String, String> stringStringMapOperator = data.map(new RichMapFunction<String, String>() {
            Collection<Integer> broadcastSet = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                // 第三步访问集合形式的广播变量数据集
                broadcastSet = getRuntimeContext().getBroadcastVariable("broadcastSetName");
            }

            @Override
            public String map(String value) throws Exception {
                for (Integer integer : broadcastSet) {
                    System.out.println(integer);
                }
                return value;
            }
        }).withBroadcastSet(toBroadcast, "broadcastSetName");// 第二步广播数据集

        stringStringMapOperator.printToErr();
        env.execute(WindowWordCount.class.getName());
    }


    private static void Stream(StreamExecutionEnvironment env) throws Exception {
        DataStream<Tuple2<String, Integer>> dataStream = env
                .socketTextStream("localhost", 9999)
                .flatMap(new Splitter())
                .keyBy(0)
                .timeWindow(Time.seconds(5))
                .sum(1);

        dataStream.print();
        env.execute("Window WordCount");
    }


    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word: sentence.split("\\s+")) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }
}