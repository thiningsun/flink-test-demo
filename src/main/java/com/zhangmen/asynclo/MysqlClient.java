package com.zhangmen.asynclo;

import java.sql.*;

public class MysqlClient {
/*
    private static String jdbcUrl = "jdbc:mysql://localhost:3306?serverTimezone=GMT%2B8&useUnicode=true&characterEncoding=utf-8";

    private static String username = "root";
    private static String password = "root";
    private static String driverName = "com.mysql.jdbc.Driver";
    public  java.sql.Connection conn;
    private static PreparedStatement ps;
//    private static Statement statement;

    static {
        try {
            Class.forName(driverName);
            conn = DriverManager.getConnection(jdbcUrl, username, password);
//            statement = conn.createStatement();
//            ps = conn.prepareStatement("select username from mytest.user where id = ?");
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }*/


    /**
     * execute query
     *
     * @param user
     * @return
     */

    /**
     * execute query
     *
     * @param user
     * @return
     */
    public AsyncUser query1(AsyncUser user, java.sql.Connection conn) throws SQLException {
        System.out.println("conn地址:" + conn.toString());
        Statement statement = conn.createStatement();
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        String phone = "0000";
        try {
            ResultSet rs = statement.executeQuery("select id,name ,age from mytest.user where id = " + user.getId());
            if (rs.next()) {
                phone = rs.getString(2);
                user.setPhone(phone);
            }
            System.out.println("execute query : " + user.getId() + "-2-" + "phone : " + phone + "-" + System.currentTimeMillis());
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return user;
    }

}
