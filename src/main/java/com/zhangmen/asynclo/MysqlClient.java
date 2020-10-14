package com.zhangmen.asynclo;

import java.sql.*;

public class MysqlClient {

    private static String jdbcUrl = "jdbc:mysql://localhost:3306?useSSL=false&allowPublicKeyRetrieval=true";
    private static String username = "root";
    private static String password = "root";
    private static String driverName = "com.mysql.jdbc.Driver";
    private static java.sql.Connection conn;
    private static PreparedStatement ps;
    private static Statement statement;

    static {
        try {
            Class.forName(driverName);
            conn = DriverManager.getConnection(jdbcUrl, username, password);
            statement = conn.createStatement();
//            ps = conn.prepareStatement("select username from mytest.user where id = ?");
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

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
    public AsyncUser query1(AsyncUser user) {
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        String phone = "0000";
        try {
            ResultSet rs = statement.executeQuery("select username,password from mytest.user where id = " + user.getId());
//            ps.setString(1, user.getId());
//            ResultSet rs = ps.executeQuery();
            if (!rs.isClosed() && rs.next()) {
                phone = rs.getString(2);
                user.setPhone(phone);
            }
            System.out.println("execute query : " + user.getId() + "-2-" + "phone : " + phone + "-" + System.currentTimeMillis());
        } catch (SQLException e) {
            e.printStackTrace();
        }
        user.setPhone(phone);
        return user;
    }

}
