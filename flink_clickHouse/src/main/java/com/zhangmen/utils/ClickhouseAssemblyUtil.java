package com.zhangmen.utils;

import org.apache.commons.lang3.StringUtils;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * @author jinxing.zhang
 */
public class ClickhouseAssemblyUtil {

    public static DateTimeFormatter DATE_FORMATTER = DateTimeFormatter
            .ofPattern("yyyy-MM-dd HH:mm:ss");

    /**
     * 拼接字符串类型
     * @param value
     * @param builder
     * @param isEnd
     */
    public static void buildString(String value,StringBuilder builder,boolean isEnd){
        if(StringUtils.isNotEmpty(value)){
            builder.append("'");
            builder.append(value);
            builder.append("'");
        }else {
            builder.append(value);
        }
        if(!isEnd){
            builder.append(", ");
        }
    }

    /**
     * 拼接数字类型
     * @param value
     * @param builder
     * @param isEnd
     */
    public static void buildNumber(Number value,StringBuilder builder,boolean isEnd){
        builder.append(value);
        if(!isEnd){
            builder.append(", ");
        }
    }

    /**
     * 拼接数字类型
     * @param value
     * @param builder
     * @param isEnd
     */
    public static void buildDate(Date value, StringBuilder builder, boolean isEnd){
        String dateValue = value.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime().format(DATE_FORMATTER);
        buildString(dateValue, builder, isEnd);
    }
}
