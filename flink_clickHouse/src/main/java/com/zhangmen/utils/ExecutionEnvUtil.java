package com.zhangmen.utils;

import com.zhangmen.context.ClickhouseConfig;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

public class ExecutionEnvUtil {

    public static StreamExecutionEnvironment prepare() {

        // 初始化执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置clickhouse 参数
        ParameterTool parameters = ParameterTool.fromMap(ClickhouseConfig.buildClickHouseParameters());
        env.getConfig().setGlobalJobParameters(parameters);

        CheckpointConfig checkpointConf = env.getCheckpointConfig();
        //插入检查点时间隔,同时设置检查模式为一次语义
        env.enableCheckpointing(6000, CheckpointingMode.EXACTLY_ONCE);
        //设置检查点的周期，每隔6s启动一个检查点
        checkpointConf.setCheckpointInterval(6000);
        //设置检查点制作的超时时间1min
        checkpointConf.setCheckpointTimeout(1000);
        //设置检查点制作失败是否停止程序
        checkpointConf.setFailOnCheckpointingErrors(false);
        //同一时间只允许进行一个检查点
        checkpointConf.setMaxConcurrentCheckpoints(2);
        checkpointConf.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //设置检查点的重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000L));
        env.setStateBackend(new FsStateBackend("file:///D://workspace//flink_arlen_test//data", true));
        // 设置stateBackend
        /*if (StringUtils.isNotBlank(ClickhouseConfig.checkpointDataUri)) {
            RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend("file:///D://workspace//flink_arlen_test//data", true);
            rocksDBStateBackend.setPredefinedOptions(PredefinedOptions.SPINNING_DISK_OPTIMIZED_HIGH_MEM);
            env.setStateBackend((StateBackend) rocksDBStateBackend);
        }*/
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, org.apache.flink.api.common.time.Time.of(30, TimeUnit.SECONDS)));
        //5s做一次checkpoint
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        return env;
    }
}
