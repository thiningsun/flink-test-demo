package com.zhangmen.sql;

import com.zhangmen.bean.User;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Desc: 自定义 source，从 mysql 中读取数据
 */
public class SourceFromMySQL extends RichSourceFunction<User> {

    PreparedStatement ps;
    private Connection connection;
    private ParameterTool parameterTool;
    private volatile boolean isRunning = true;

    /**
     * open() 方法中建立连接，这样不用每次 invoke 的时候都要建立连接和释放连接。
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        connection = MySQLUtil.getConnection();
        String sql = "select * from mytest.user;";
        if (connection != null) {
            ps = this.connection.prepareStatement(sql);
        }
    }

    /**
     * 程序执行完毕就可以进行，关闭连接和释放资源的动作了
     *
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) { //关闭连接和释放资源
            connection.close();
        }
        if (ps != null) {
            ps.close();
        }
    }

    /**
     * DataStream 调用一次 run() 方法用来获取数据
     * @param ctx
     * @throws Exception
     */
    @Override
    public void run(SourceContext<User> ctx) throws Exception {
        while (isRunning) {
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                User user = User.builder().id(resultSet.getInt("id"))
                        .name(resultSet.getString("name"))
                        .age(resultSet.getString("age"))
                        .email(resultSet.getString("email")).build();
                ctx.collect(user);
                ctx.close();
            }

            Thread.sleep(1000 * 10);
            System.out.println("10s更新一次 ");
            //Thread.sleep(1000 * 60 * 60 * 24); //24H 更新一次
        }
    }

    @Override
    public void cancel() {
    }
}
