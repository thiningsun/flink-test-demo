package com.zhangmen.arlen;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

public class WordCountByTableAPI {

   /* // FLINK STREAMING QUERY  tableAPI执行环境初始化
    EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
    StreamExecutionEnvironment fsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironment fsTableEnv = StreamTableEnvironment.create(fsEnv, fsSettings);

    DataStream<String> sourceDS = fsEnv.socketTextStream("localhost", 9000);
    Table table = fsTableEnv.fromDataStream(sourceDS,"inputLine");
    // 注册自定义UDF函数
    fsTableEnv.registerFunction("wordCountUDF", new UDFWordCount());


    Table wordCount= table.joinLateral("wordCountUDF(inputLine) as (word, countOne)")
            .groupBy("word")
            .select("word, countOne.sum as countN");
    table 转换DataStream
    DataStream<Tuple2<Boolean, Row>> result = fsTableEnv.toRetractStream(wordCount, Types.ROW(Types.STRING, Types.INT));

    统计后会在 统计记录前面加一个true  false标识 这里你可以注释掉跑下看看 对比下
        result.filter(new FilterFunction<Tuple2<Boolean, Row>>() {
        @Override
        public boolean filter(Tuple2<Boolean, Row> value) throws Exception {
            if(value.f0==false){
                return false;
            }else{
                return true;
            }
        }
    }).print();
        fsEnv.execute();*/
}
