package com.zhangmen.flinkkafka;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class FlinkCheckpoint {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CheckpointConfig checkpointConf = env.getCheckpointConfig();
        //插入检查点时间隔
        env.enableCheckpointing(6000);
        //设置检查模式为一次语义
        checkpointConf.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //设置检查点的周期，每隔6s启动一个检查点
        checkpointConf.setCheckpointInterval(6000);
        //设置检查点制作的超时时间1min
        checkpointConf.setCheckpointTimeout(1000);
        //设置检查点制作失败是否停止程序
        checkpointConf.setFailOnCheckpointingErrors(false);
        //同一时间只允许进行一个检查点
        checkpointConf.setMaxConcurrentCheckpoints(2);
        checkpointConf.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000L));
        //设置检查点的重启策略
        env.setStateBackend(new FsStateBackend("file:///D://workspace//flink_arlen_test//data", true));
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, org.apache.flink.api.common.time.Time.of(30, TimeUnit.SECONDS)));
        //5s做一次checkpoint
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.setParallelism(4);
        String topic = "zsq_test";

        //source
        FlinkKafkaConsumer011<String> kafkaConsumer = new FlinkKafkaConsumer011<String>(topic, new SimpleStringSchema(), getKafkaProperties());
        kafkaConsumer.setStartFromLatest();
//        kafkaConsumer.setStartFromGroupOffsets();
        DataStreamSource<String> dataStreamSource = env.addSource(kafkaConsumer);
        SingleOutputStreamOperator<String> stringSingleOutputStreamOperator = dataStreamSource.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(5)) {
            @Override
            public long extractTimestamp(String element) {
                String[] split = element.split(",");
                return Long.parseLong(split[2]);
            }
        });

        stringSingleOutputStreamOperator.print();
        env.execute("FlinkCheckpoint");
    }

    private static Properties getKafkaProperties() {
        Properties properties = new Properties();
//        properties.setProperty("bootstrap.servers", "10.81.176.89:9092");
        properties.setProperty("bootstrap.servers", "172.31.117.101:9092");
        properties.setProperty("group.id", "arlen-group-02");
        return properties;
    }
}
