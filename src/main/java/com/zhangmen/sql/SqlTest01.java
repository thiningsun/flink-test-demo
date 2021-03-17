package com.zhangmen.sql;

import com.zhangmen.bean.User;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.expressions.Round;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Random;

import static org.apache.flink.table.api.Expressions.$;

public class SqlTest01 {
    public static void main(String[] args) throws Exception {
        // set up execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // ingest a DataStream from an external source
        DataStream<Tuple3<Long, String, Long>> ds = env.addSource(new SourceFunction<Tuple3<Long, String, Long>>() {
            @Override
            public void run(SourceContext<Tuple3<Long, String, Long>> out) throws Exception {
                Long id = 1L;
                while (true){
                    Long milliSecond = LocalDateTime.now().toInstant(ZoneOffset.of("+8")).toEpochMilli();
                    Random random = new Random();
                    out.collect(new Tuple3<>(id,"a",Long.valueOf(random.nextInt(10))));
                    Thread.sleep(200L);
                    id++;
                }
            }
            public void cancel() {
            }
        }).setParallelism(1);


        DataStreamSource<User> mysqlStream = env.addSource(new SourceFromMySQL());
        mysqlStream.print();

        Table table = tableEnv.fromDataStream(ds, $("id1"), $("product"), $("amount"));
        tableEnv.createTemporaryView("flink", table);

        Table table2 = tableEnv.fromDataStream(mysqlStream, $("id"), $("name"), $("age"),$("email"));
        tableEnv.createTemporaryView("mysql", table2);

//        Table table2 = tableEnv.fromDataStream(ds, $("user"), $("product"), $("amount"));
//        tableEnv.createTemporaryView("flink2", table2);

        Table table1 = tableEnv.sqlQuery("select flink.*,mysql.* from flink inner join mysql on flink.amount=mysql.id");

//        Table mysql = tableEnv.sqlQuery("select * from mysql");
//        Table table1 = tableEnv.scan("flink").join(mysql, "id=amount");


        tableEnv.toRetractStream(table1, Row.class).printToErr();

        /*DataStream<Tuple2<Boolean, Row>> dsRow = tableEnv.toRetractStream(table, Row.class);
        dsRow.print();*/

        env.execute();
    }



}
