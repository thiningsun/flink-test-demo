package com.zhangmen.asynclo;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.DriverManager;
import java.util.Collections;

public class AsyncFunctionForMysqlJava02 extends RichAsyncFunction<AsyncUser, AsyncUser> {

    private static String jdbcUrl = "jdbc:mysql://localhost:3306?serverTimezone=GMT%2B8&useUnicode=true&characterEncoding=utf-8";
    private static String username = "root";
    private static String password = "root";
    private static String driverName = "com.mysql.jdbc.Driver";

    Logger logger = LoggerFactory.getLogger(AsyncFunctionForMysqlJava02.class);
    private transient MysqlClient client;
    public java.sql.Connection conn;

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
        conn = DriverManager.getConnection(jdbcUrl, username, password);
    }

    @Override
    public void asyncInvoke(AsyncUser input, ResultFuture<AsyncUser> resultFuture) throws Exception {
        AsyncUser tmp = client.query1(input, conn);
        resultFuture.complete(Collections.singletonList(tmp));
    }

    @Override
    public void timeout(AsyncUser input, ResultFuture<AsyncUser> resultFuture) throws Exception {

    }
}
