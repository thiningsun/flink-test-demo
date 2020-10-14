package com.zhangmen.asynclo;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.util.concurrent.TimeUnit;

public class AsyncMysqlRequest {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
        DataStreamSource<String> source = env.readTextFile("D:\\workspace\\flink_arlen_test\\data\\async.txt");

        // 接收kafka数据，转为User 对象
        DataStream<AsyncUser> input = source.map(value -> {
            JSONObject json = JSON.parseObject(value);
            String id = json.get("id").toString();
            String username = json.get("username").toString();
            String password = json.get("password").toString();

            return new AsyncUser(id, username, password);
        });

        // 异步IO 获取mysql数据, timeout 时间 1s，容量 10（超过10个请求，会反压上游节点）
        SingleOutputStreamOperator<AsyncUser> async = AsyncDataStream.
                orderedWait(input,
                new AsyncFunctionForMysqlJava02(),
                5000,
                TimeUnit.MICROSECONDS,
                5);

        async.map(user -> JSON.toJSON(user).toString()+"========").print();

        env.execute("asyncForMysql");

    }
}
