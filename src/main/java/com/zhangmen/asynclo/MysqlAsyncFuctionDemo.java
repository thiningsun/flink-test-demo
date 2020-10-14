package com.zhangmen.asynclo;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public class MysqlAsyncFuctionDemo extends RichAsyncFunction<AsyncUser,AsyncUser> {

    private static String jdbcUrl = "jdbc:mysql://localhost:3306?useUnicode=true&characterEncoding=utf-8&useSSL=false";
    private static String username = "root";
    private static String password = "123456";
    private static String driverName = "com.mysql.jdbc.Driver";
    private static java.sql.Connection conn;
    private static PreparedStatement ps;
    private static SimpleDateFormat sdf;

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(driverName);
        conn = DriverManager.getConnection(jdbcUrl, username, password);
        ps = conn.prepareStatement("select phone from ke.async_test where id = ?");
        sdf = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss");
    }

    @Override
    public void asyncInvoke(AsyncUser input, ResultFuture<AsyncUser> resultFuture) throws Exception {
        CompletableFuture.supplyAsync(new Supplier<AsyncUser>() {
            @Override
            public AsyncUser get() {
                try {
                    ps.setString(1,input.getId());
                    ResultSet rs = ps.executeQuery();
                    if(!rs.isClosed() && rs.next()){
                      String  phone = rs.getString(1);
                        input.setPhone(phone);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
                System.out.println(input);
                return input;
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

        });

    }

    @Override
    public void timeout(AsyncUser input, ResultFuture<AsyncUser> resultFuture) throws Exception {
        Thread.sleep(1000L);
        List<AsyncUser> list = new ArrayList();
        input.setPhone("timeout");
        list.add(input);
        String format = sdf.format(System.currentTimeMillis());
        System.out.println("++++++++++++++++"+format);
        resultFuture.complete(list);

    }

    @Override
    public void close() throws Exception {
//        ps.close();
//        conn.close();
        super.close();
    }
}

