package com.zhangmen;

import com.zhangmen.context.Constant;
import com.zhangmen.functions.map.ClickHouseMapFunction;
import com.zhangmen.functions.sink.ClickhouseSinkDelegation;
import com.zhangmen.functions.source.kafkaSource;
import com.zhangmen.utils.ExecutionEnvUtil;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink2ClickHouse {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare();//获取执行环境，配置参数
        String topic = Constant.TOPIC;
        String tableName = Constant.TABLENAME;

        SingleOutputStreamOperator<String> mapOperator = env
                .setParallelism(1)
                .addSource(kafkaSource.buildSource(topic))
                .name("load flink source " + topic)
                .uid("load flink source " + topic)
                .map(new ClickHouseMapFunction())
                .name("map flink source " + topic)
                .uid("map flink source " + topic);

        mapOperator.addSink(ClickhouseSinkDelegation.sink(tableName))
                .name("sink flink source " + topic)
                .uid("sink flink source " + topic);

        env.execute("FlinkCheckpoint");
    }
}
