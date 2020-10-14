package com.zhangmen.test;

import com.alibaba.excel.EasyExcel;
import org.apache.flink.shaded.guava18.com.google.common.base.Joiner;
import org.junit.Test;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author jinxing.zhang
 */
public class TestDemo {

    private static DateTimeFormatter DATE_FORMATTER = DateTimeFormatter
            .ofPattern("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) {
        int httpCode[] = {200,400,500,413,417,404,503};
        AtomicLong counter = new AtomicLong(1);
        ThreadLocalRandom random = ThreadLocalRandom.current();
        List<TrackApmNetwork> networkList = new ArrayList<>(10000000);
        for (int i = 0; i < 10000; i++) {
            TrackApmNetwork network = TrackApmNetwork.builder()
                    .appId("11003")
                    .deviceId(UUID.randomUUID().toString())
                    .trackerType(11)
                    .time(System.currentTimeMillis())
                    .counter((int) counter.incrementAndGet())
                    .serverTime(System.currentTimeMillis())
                    .scene(1)
                    .cSource("ios")
                    .ip("192.168.44.147")
                    .method("post")
                    .protocol("http")
                    .localQueueTime(random.nextInt(100))
                    .bytesSent(random.nextLong(10000))
                    .url("http://www.baidu.com")
                    .duration(random.nextInt(100))
                    .bytesReceived(random.nextLong(10000))
                    .dnsTime(random.nextInt(10))
                    .remainPackageTime(random.nextInt(1000))
                    .firstPacketTime(random.nextInt(10))
                    .sslTime(random.nextInt(10))
                    .httpLibType("OKhttp")
                    .httpStatusCode(httpCode[random.nextInt(6)])
                    .build();
            network.setClientTransId(MD5Util.encrypt(Joiner.on("|").skipNulls().join(network.getTime(), network.getCounter(), network.getDeviceId(), network.getCSource())));
            network.setClientTime(LocalDateTime.now().withMinute(0).withSecond(0).format(DATE_FORMATTER));
            networkList.add(network);
        }
//        D:\User\Desktop\docker基本命令.txt
        String fileName = "D:\\User\\Desktop\\testData.xlsx";
        EasyExcel.write(fileName, TrackApmNetwork.class).sheet("模板").doWrite(networkList);

    }

    @Test
    public void test01() {
        DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        String now = LocalDateTime.now().format(DATE_FORMATTER);
        System.out.println(now);

    }
}
