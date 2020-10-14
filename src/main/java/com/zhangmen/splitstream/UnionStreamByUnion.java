package com.zhangmen.splitstream;


import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Author: Wang Pei
 * Summary:
 *  合流：基于Union
 */
@Slf4j
public class UnionStreamByUnion {
    public static void main(String[] args) throws Exception {

            /**运行环境*/
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            /**输入数据源source1*/
            DataStreamSource<Tuple3<String, String, String>> source1 = env.fromElements(
                    new Tuple3<>("productID1", "click", "user_1")
            );

            /**输入数据源source2*/
            DataStreamSource<Tuple3<String, String, String>> source2 = env.fromElements(
                    new Tuple3<>("productID3", "click", "user_1"),
                    new Tuple3<>("productID3", "click", "user_2")
            );

            /**输入数据源source3*/
            DataStreamSource<Tuple3<String, String, String>> source3 = env.fromElements(
                    new Tuple3<>("productID2", "browse", "user_1"),
                    new Tuple3<>("productID2", "click", "user_2"),
                    new Tuple3<>("productID2", "click", "user_1")
            );

            /**合并流*/
            source1.union(source2,source3).print();

            env.execute();

        }
}
