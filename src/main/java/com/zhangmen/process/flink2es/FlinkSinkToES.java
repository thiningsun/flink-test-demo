package com.zhangmen.process.flink2es;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch.util.RetryRejectedExecutionFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Requests;

import java.util.*;

public class FlinkSinkToES {
    private static final String READ_TOPIC = "zsq-test01";

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //周期性的发送检查点
        env.enableCheckpointing(6000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointInterval(6000);
        env.getCheckpointConfig().setFailOnCheckpointingErrors(false);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        //需要手动删除检查点
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //设置事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime); //消息源本身的时间

        Properties props = new Properties();
        props.put("bootstrap.servers", "172.31.117.101:9092");
        props.put("zookeeper.connect", "172.31.117.101:2181");
        props.put("group.id", "student-group-1");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        props.put("auto.offset.reset", "latest");
//        props.put("auto.offset.reset", "earliest");

        FlinkKafkaConsumer011<String> kafkaConsumer011 = new FlinkKafkaConsumer011<>(
                //这个 kafka topic 需要和上面的工具类的 topic 一致
                READ_TOPIC,
                new SimpleStringSchema(),
                props);
        kafkaConsumer011.setStartFromEarliest();

        DataStreamSource<String> student = env.addSource(kafkaConsumer011).setParallelism(5);
//                .map(string -> JSON.parseObject(string, Student.class)); //Fastjson 解析字符串成 student 对象

        student.print();
//        log.info("student:" + student);

        List<HttpHost> esHttphost = new ArrayList<>();
        esHttphost.add(new HttpHost("zmbd-uat08", 9200, "http"));
        ElasticsearchSink.Builder<String> esSinkBuilder = new ElasticsearchSink.Builder<>(
                esHttphost,
                new ElasticsearchSinkFunction<String>() {
                    public IndexRequest createIndexRequest(String element) {
                        Map<String, String> json = new HashMap<>();
                        json.put("data", element);
//                        log.info("data:" + element);

                        return Requests.indexRequest()
                                .index("index-arlen-student")
                                .type("student")
                                .source(json);

                    }

                    @Override
                    public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(element));
                    }
                }
        );

        esSinkBuilder.setBulkFlushMaxActions(1);
//        esSinkBuilder.setRestClientFactory(
//                restClientBuilder -> {
//                    restClientBuilder.setDefaultHeaders()
//                }
//        );
//        esSinkBuilder.setRestClientFactory(new RestClientFactoryImpl());
        esSinkBuilder.setFailureHandler(new RetryRejectedExecutionFailureHandler());
        student.addSink(esSinkBuilder.build());
        env.execute("flink learning connectors kafka");



    }

}
