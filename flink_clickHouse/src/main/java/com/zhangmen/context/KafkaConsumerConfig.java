package com.zhangmen.context;

import com.typesafe.config.Config;
import com.zhangmen.utils.ConfigUtil;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.util.Properties;


public class KafkaConsumerConfig {

    final static Config config = ConfigUtil.getProperties();//获取执行环境
    public  static String servers=config.getString("--kafka.consumer.servers");


    /**
     * 自动提交
     */
    public  static String enableAutoCommit=config.getString("--kafka.consumer.enable.auto.commit");
    public  static String autoCommitInterval=config.getString("--kafka.consumer.auto.commit.interval");
    public  static String sessionTimeout=config.getString("--kafka.consumer.session.timeout");
    public  static String requestTimeout=config.getString("--kafka.consumer.request.timeout");
    public  static String receiveBufferBytes=config.getString("--kafka.consumer.receive.buffer.bytes");

    /**
     * 默认消费者组
     */
    public  static String groupId=config.getString("--kafka.consumer.group.id");
    public  static String autoOffsetReset=config.getString("--kafka.consumer.auto.offset.reset");

    public static Properties kafkaProperties() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, autoCommitInterval);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeout);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG,requestTimeout);
        props.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG,receiveBufferBytes);
        return props;
    }

}
