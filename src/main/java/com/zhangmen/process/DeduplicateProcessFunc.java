package com.zhangmen.process;

import com.google.common.base.Stopwatch;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.hash.BloomFilter;
import org.apache.flink.shaded.guava18.com.google.common.hash.Funnels;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author jinxing.zhang
 */
@Slf4j
public class DeduplicateProcessFunc extends KeyedProcessFunction<String, Map, String> {

    private static final int BF_CARDINAL_THRESHOLD = 100000000;
    private static final double BF_FALSE_POSITIVE_RATE = 0.01;

    private transient BloomFilter<String> bloomFilter;

    @Override
    public void open(Configuration parameters) throws Exception {
        Stopwatch stopwatch = Stopwatch.createStarted();
        bloomFilter = BloomFilter.create(Funnels.stringFunnel(Charset.forName("UTF-8")), BF_CARDINAL_THRESHOLD, BF_FALSE_POSITIVE_RATE);
        log.info("Created Guava BloomFilter, time cost: {}", stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        Stopwatch stopwatch = Stopwatch.createStarted();
        bloomFilter = BloomFilter.create(Funnels.stringFunnel(Charset.forName("UTF-8")), BF_CARDINAL_THRESHOLD, BF_FALSE_POSITIVE_RATE);
        log.info("Timer triggered & resetted Guava BloomFilter, time cost: {}", stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }

    @Override
    public void close() throws Exception {
        bloomFilter = null;
    }

    @Override
    public void processElement(Map value, Context ctx, Collector<String> out) throws Exception {
        String clientTransId = (String) value.get("name");
        System.out.println("value:"+value);
        if(!bloomFilter.mightContain(clientTransId)){
            bloomFilter.put(clientTransId);
            System.out.println("bloomFilter放入数据：" + value.toString());
            out.collect(value.toString());
        }
    }
}
