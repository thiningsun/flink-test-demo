package com.zhangmen.bean;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class RoundRobinPartition implements Partitioner {

    AtomicInteger atomicInteger = new AtomicInteger(0);
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        Integer cnt = cluster.partitionCountForTopic(topic);
        int i = atomicInteger.incrementAndGet() % cnt;
        if(atomicInteger.get()> 1000){
            atomicInteger.set(0);
        }
        return i;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
