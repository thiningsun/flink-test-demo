package com.zhangmen.context;

import com.typesafe.config.Config;
import com.zhangmen.utils.ConfigUtil;

/**
 * @author jinxing.zhang
 */
public abstract class Constant {

    private final static Config config = ConfigUtil.getProperties();//获取执行环境

    public static final String CLICKHOUSE_DATABASE = "test";

    public static final String TABLENAME ="flink_test_all";

    public static final String TOPIC =config.getString("--topic");

    public static final String TOPIC_TRACK = "track";
}
