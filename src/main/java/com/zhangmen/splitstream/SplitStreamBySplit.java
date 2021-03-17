package com.zhangmen.splitstream;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.expressions.In;

import java.util.ArrayList;

/**
 * Author: Wang Pei
 * Summary:
 *  分流：基于Split-Select (过时方式：流不能复用)
 */
@Slf4j
public class SplitStreamBySplit {

    public static String flage = "productID1";
    public static void main(String[] args) throws Exception {
        /**运行环境*/
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        /**输入数据源*/
        DataStreamSource<Tuple3<String, String, String>> source = env.fromElements(
                new Tuple3<>("productID1", "click", "user_1"),
                new Tuple3<>("productID1", "click", "user_2"),
                new Tuple3<>("productID1", "browse", "user_1"),
                new Tuple3<>("productID2", "browse", "user_1"),
                new Tuple3<>("productID2", "click", "user_2"),
                new Tuple3<>("productID2", "click", "user_1")
        );



        String flage = "1";
        source.keyBy(new KeySelector<Tuple3<String, String, String>, Object>() {
            @Override
            public Object getKey(Tuple3<String, String, String> stringStringStringTuple3) throws Exception {

                return stringStringStringTuple3.getField(Integer.valueOf(flage));
            }
        }).printToErr();



        /**1、定义拆分逻辑*/
        SplitStream<Tuple3<String, String, String>> splitStream = source.split(new OutputSelector<Tuple3<String, String, String>>() {
            @Override
            public Iterable<String> select(Tuple3<String, String, String> value) {

                ArrayList<String> output = new ArrayList<>();
                if (value.f0.equals("productID1")) {
                    output.add("productID1");

                } else if (value.f0.equals("productID2")) {
                    output.add("productID2");
                }

                return output;

            }
        });

        /**2、将流真正拆分出来*/
        splitStream.select("productID1").print();

        env.execute();
    }
}
