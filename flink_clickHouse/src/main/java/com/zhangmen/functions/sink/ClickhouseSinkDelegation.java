package com.zhangmen.functions.sink;

import com.zhangmen.context.ClickhouseConfig;
import com.zhangmen.context.Constant;
import com.zhangmen.context.SeparatorConstant;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import ru.ivi.opensource.flinkclickhousesink.ClickhouseSink;
import ru.ivi.opensource.flinkclickhousesink.model.ClickhouseSinkConsts;
import java.util.Properties;

public class ClickhouseSinkDelegation {

    public static SinkFunction sink(String tableName) {
        Properties props = new Properties();
        props.put(ClickhouseSinkConsts.TARGET_TABLE_NAME, String.join(SeparatorConstant.SPOT, Constant.CLICKHOUSE_DATABASE, tableName));
        props.put(ClickhouseSinkConsts.MAX_BUFFER_SIZE, ClickhouseConfig.maxBufferSize);
        return new ClickhouseSink(props);
    }
}
