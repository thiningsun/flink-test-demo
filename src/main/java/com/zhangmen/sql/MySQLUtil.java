package com.zhangmen.sql;

import lombok.extern.slf4j.Slf4j;
import java.sql.Connection;
import java.sql.DriverManager;

/**
 * Desc: MySQL 工具类
 * Created by xinghong.gao on 2020-05-09
 */
@Slf4j
public class MySQLUtil {

    /**
     *  单一链接mysql,适合频率低的查询访问
     * @return
     */
    public static Connection getConnection() {
        String url = "jdbc:mysql://localhost:3306/mytest?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC";
        String user ="root";
        String password = "root";
        String driver = "com.mysql.jdbc.Driver";
        Connection con = null;
        try {
            Class.forName(driver);
            //注意，这里替换成你自己的mysql 数据库路径和用户名、密码
            con = DriverManager.getConnection(url, user, password);
        } catch (Exception e) {
            log.error("-----------mysql get connection has exception , msg = "+ e.getMessage());
        }
        return con;
    }

}
