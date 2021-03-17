package com.zhangmen.asynclo;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Supplier;

public class AsyncFunctionForMysqlJava extends RichAsyncFunction<AsyncUser, AsyncUser> {


    Logger logger = LoggerFactory.getLogger(AsyncFunctionForMysqlJava.class);
    private transient MysqlClient client;
    private transient ExecutorService executorService;
    private SimpleDateFormat sdf;


    /**
     * open 方法中初始化链接
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        logger.info("async function for mysql java open ...");
        super.open(parameters);
        client = new MysqlClient();
        executorService = Executors.newFixedThreadPool(5);
         sdf = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss");
    }

    /**
     * use asyncUser.getId async get asyncUser phone
     *
     * @param asyncUser
     * @param resultFuture
     * @throws Exception
     */
    @Override
    public void asyncInvoke(AsyncUser asyncUser, ResultFuture<AsyncUser> resultFuture) throws Exception {

        System.out.println(asyncUser.getId());
        System.out.println(asyncUser.getUsername());

        executorService.submit(() -> {
            // submit query
            System.out.println("submit query : " + asyncUser.getId() + "-1-" + System.currentTimeMillis());
            AsyncUser tmp = null;
            /*try {
                tmp = client.query1(asyncUser);
            } catch (SQLException e) {
                e.printStackTrace();
            }*/
            //            return tmp;
            // 一定要记得放回 resultFuture，不然数据全部是timeout 的
            resultFuture.complete(Collections.singletonList(tmp));
        });

        executorService.shutdown();

        /*CompletableFuture.supplyAsync(new Supplier<AsyncUser>() {
            @Override
            public AsyncUser get() {
                AsyncUser b = client.query1(asyncUser);
//                System.out.println(b);
                return b;
            }
        }).thenAccept((AsyncUser a )->{
            try {
                Thread.sleep(1000L);
                String format = sdf.format(System.currentTimeMillis());
                System.out.println("---------------"+format);
                resultFuture.complete(Collections.singleton(a));
            } catch (Exception e) {
                e.printStackTrace();
            }

        });*/
    }

    @Override
    public void timeout(AsyncUser input, ResultFuture<AsyncUser> resultFuture) throws Exception {
        logger.warn("Async function for mysql timeout");
        Thread.sleep(1000L);
        List<AsyncUser> list = new ArrayList();
        input.setPhone("timeout");
        list.add(input);
        String format = sdf.format(System.currentTimeMillis());
        System.out.println("++++++++++++++++"+format);
        resultFuture.complete(list);
    }



    /**
     * close function
     *
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        logger.info("async function for mysql java close ...");
        super.close();
    }
}
