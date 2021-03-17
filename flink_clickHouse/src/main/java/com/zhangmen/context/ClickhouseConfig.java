package com.zhangmen.context;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.zhangmen.utils.ConfigUtil;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import ru.ivi.opensource.flinkclickhousesink.model.ClickhouseClusterSettings;
import ru.ivi.opensource.flinkclickhousesink.model.ClickhouseSinkConsts;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@Data
public class ClickhouseConfig {

    final static Config config = ConfigUtil.getProperties();//获取执行环境

    public  static String host=config.getString("--clickhouse.access.hosts");
    public  static String user=config.getString("--clickhouse.access.user");
    public  static String password=config.getString("--clickhouse.access.password");
    public  static String maxBufferSize=config.getString("--clickhouse.sink.max-buffer-size");
    public  static String numWriters=config.getString("--clickhouse.sink.num-writers");
    public  static String queueMaxCapacity=config.getString("--clickhouse.sink.queue-max-capacity");
    public  static String timeoutSec=config.getString("--clickhouse.sink.timeout-sec");
    public  static String retries=config.getString("--clickhouse.sink.retries");
    public  static String failedRecordsPath=config.getString("--clickhouse.sink.failed-records-path");

    public  static String checkpointDataUri=config.getString("--fs.state.backend.uri");

    public static Map<String, String> buildClickHouseParameters(){
        Map<String, String> globalParameters = new HashMap<>();

        // clickhouse cluster properties
        globalParameters.put(ClickhouseClusterSettings.CLICKHOUSE_HOSTS, host);
        globalParameters.put(ClickhouseClusterSettings.CLICKHOUSE_USER, user);
        globalParameters.put(ClickhouseClusterSettings.CLICKHOUSE_PASSWORD, password);

        // sink common
        globalParameters.put(ClickhouseSinkConsts.TIMEOUT_SEC, timeoutSec);
        globalParameters.put(ClickhouseSinkConsts.FAILED_RECORDS_PATH, failedRecordsPath);
        globalParameters.put(ClickhouseSinkConsts.NUM_WRITERS, numWriters);
        globalParameters.put(ClickhouseSinkConsts.NUM_RETRIES, retries);
        globalParameters.put(ClickhouseSinkConsts.QUEUE_MAX_CAPACITY, queueMaxCapacity);

        return globalParameters;
    }

}
