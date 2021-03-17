package com.zhangmen.functions.source;

import com.zhangmen.context.KafkaConsumerConfig;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

public class kafkaSource {

    public static SourceFunction<String> buildSource(String topic) {
        //sourceKafka
        FlinkKafkaConsumer011<String> kafkaConsumer = new FlinkKafkaConsumer011<String>(topic, new SimpleStringSchema(), KafkaConsumerConfig.kafkaProperties());
        kafkaConsumer.setStartFromLatest();
        kafkaConsumer.setCommitOffsetsOnCheckpoints(true);
        return kafkaConsumer;
    }
}
