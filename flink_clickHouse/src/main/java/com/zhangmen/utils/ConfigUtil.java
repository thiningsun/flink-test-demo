package com.zhangmen.utils;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.commons.lang3.StringUtils;
import java.util.Objects;

public class ConfigUtil {

    final static String env = ConfigFactory.load("application").getString("env.profile");
    final static Config config = ConfigFactory.load(getConfig(env));//获取执行环境

    //获取配置文件
    public static String getConfig(String env) {
        if ("".equals(env) || Objects.isNull(env)) {
            env = "dev";
        }
        return StringUtils.join("application-", env);
    }

    public static Config getProperties() {
        return config;
    }

}
