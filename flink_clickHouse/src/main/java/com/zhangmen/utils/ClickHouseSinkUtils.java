package com.zhangmen.utils;

import com.zhangmen.bean.User;

public class ClickHouseSinkUtils {

    public static String convertToCsv(User user){
        StringBuilder builder = new StringBuilder();
        builder.append("(");
        ClickhouseAssemblyUtil.buildString(user.getId(), builder, false);
        ClickhouseAssemblyUtil.buildString(user.getUserName(), builder,false);
        ClickhouseAssemblyUtil.buildString(user.getAge(), builder,false);
        ClickhouseAssemblyUtil.buildString(user.getSex(), builder,false);
        ClickhouseAssemblyUtil.buildString(user.getTime(), builder, true);
        builder.append(",").append("now()");
        builder.append(" )");
        return builder.toString();
    }
}
