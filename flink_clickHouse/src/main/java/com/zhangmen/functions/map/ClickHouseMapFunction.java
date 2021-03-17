package com.zhangmen.functions.map;

import com.alibaba.fastjson.JSON;
import com.zhangmen.bean.User;
import com.zhangmen.utils.ClickHouseSinkUtils;
import org.apache.flink.api.common.functions.RichMapFunction;

public class ClickHouseMapFunction extends RichMapFunction<String, String> {

    @Override
    public String map(String jsonStr){
        System.out.println("jsonStr :"+jsonStr);
        User hostsVo = JSON.parseObject(jsonStr,User.class );
        return ClickHouseSinkUtils.convertToCsv(hostsVo);
    }
}
