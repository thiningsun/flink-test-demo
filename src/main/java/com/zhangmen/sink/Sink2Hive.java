package com.zhangmen.sink;


import org.apache.flink.table.api.*;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * @Author zsq
 * @create 2021/2/5 11:55
 * @Description:
 */
public class Sink2Hive {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        String name            = "hive";
        String defaultDatabase = "test";
        String hiveConfDir     = "D:\\workspace\\flink_arlen_test\\src\\main\\resources\\hiveConf";
        String version         = "1.2.1";

        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir, version);
        tableEnv.registerCatalog("hive", hive);
        tableEnv.registerCatalog(name, hive);
        tableEnv.useCatalog(name);

        tableEnv.sqlQuery("select * from test.t_test limit 10").execute();

//        tableEnv.sqlQuery("select * from t_test limit 10").select("product_id");
//        tableEnv.sqlUpdate("insert into tmp.tmp_flink_test_2 values ('newKey')");
//        tableEnv.execute("insert into tmp");


    }
}
