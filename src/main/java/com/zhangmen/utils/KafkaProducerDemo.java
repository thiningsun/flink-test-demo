package com.zhangmen.utils;

import com.alibaba.fastjson.JSON;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.zhangmen.bean.Student;
import org.apache.kafka.clients.producer.*;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class KafkaProducerDemo extends Thread{
    private KafkaProducer<Integer, String> kafkaProducer;
    private String topic;
    private boolean isAsync;
    private AtomicInteger atomicInteger = new AtomicInteger(0);//并发计数器
    public KafkaProducerDemo(){

    }

    public KafkaProducerDemo(String topic, boolean isAysnc) {
        Config config = ConfigFactory.load("application");
        //配置kafka生产者的属性配置
        Properties properties = new Properties();
        //集群broker地址，多个broker地址逗号隔开
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("kafka.producer.servers"));
        System.out.println(config.getString("kafka.producer.servers"));
        //错误失败次数
        properties.put(ProducerConfig.RETRIES_CONFIG, config.getString("kafka.producer.retries"));
        //设置批量发送消息的size
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, config.getString("kafka.producer.batch.size"));
        //延迟发送的时间，延迟时间内的消息一起发送到broker'
        properties.put(ProducerConfig.LINGER_MS_CONFIG, config.getString("kafka.producer.linger"));
        //最大缓存
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, config.getString("kafka.producer.buffer.memory"));
        //设置生产者id
        properties.put(ProducerConfig.CLIENT_ID_CONFIG,"KafkaProducerDemo");
        //设置发送消息ack模式
        properties.put(ProducerConfig.ACKS_CONFIG,"-1");
        //每次请求最大的字节数
        properties.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 4096);

//        properties.put("group.id", "arlen-test");
        //key序列化类
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.IntegerSerializer");
        //value序列化类
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        //自定义分区器
         properties.put("partitioner.class", "com.zhangmen.bean.RoundRobinPartition");

        kafkaProducer=new KafkaProducer<Integer, String>(properties);
        this.topic=topic;
        this.isAsync=isAysnc;
    }

    @Override
    public void run() {
        int count = 10;
            //同步发送消息,get是阻塞进行的
            while (count > 0) {
                try {
                    for (int i = 1; i <= 50; i++) {
                        Student student = new Student(i, "zhangsan" + i, "password" + i, 18 + i);
                        ProducerRecord record = new ProducerRecord<String, String>(topic, JSON.toJSONString(student));
                        RecordMetadata recordMetadata = (RecordMetadata) kafkaProducer.send(record).get();
                        System.out.println(Thread.currentThread().getName()+",分区"+recordMetadata.partition()+",偏移"+recordMetadata.offset());
                        System.out.println("发送数据: " + JSON.toJSONString(student));
//                        Thread.sleep(100L);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
                count--;
            }
    }


    public static void main(String[] args) {
        String topic = "zsq-test01";  //kafka topic 需要和 flink 程序用同一个 topic
        KafkaProducerDemo kafkaProducerDemo = new KafkaProducerDemo(topic, false);
        kafkaProducerDemo.start();

    }
}
