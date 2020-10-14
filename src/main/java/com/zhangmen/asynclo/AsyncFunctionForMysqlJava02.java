package com.zhangmen.asynclo;

import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.concurrent.ExecutorService;


public class AsyncFunctionForMysqlJava02 implements AsyncFunction<AsyncUser, AsyncUser> {

    Logger logger = LoggerFactory.getLogger(AsyncFunctionForMysqlJava02.class);
    private transient MysqlClient client;
    private transient ExecutorService executorService;
    private SimpleDateFormat sdf;


    @Override
    public void asyncInvoke(AsyncUser input, ResultFuture<AsyncUser> resultFuture) throws Exception {
        client = new MysqlClient();
        AsyncUser tmp = client.query1(input);
        resultFuture.complete(Collections.singletonList(tmp));
    }

    @Override
    public void timeout(AsyncUser input, ResultFuture<AsyncUser> resultFuture) throws Exception {

    }
}
