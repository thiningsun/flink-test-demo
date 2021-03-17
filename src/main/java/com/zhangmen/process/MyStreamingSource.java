/*
package com.zhangmen.process;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zhangmen.bean.Item;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;

public class MyStreamingSource implements SourceFunction<Item> {
    private boolean isRunning = true;

    */
/**
     * 重写run方法产生一个源源不断的数据发送源
     * @param ctx
     * @throws Exception
     *//*

    @Override
    public void run(SourceContext<com.zhangmen.bean.Item> ctx) throws Exception {
        while(isRunning){
            Item item = generateItem();
            ctx.collect(item);
            //每秒产生一条数据
            Thread.sleep(1000);
        }
    }
    @Override
    public void cancel() {
        isRunning = false;
    }


    //随机产生一条商品数据
    public Item generateItem() {
//        int i = new Random().nextInt(5);
        Item item = new Item();
        item.setId(new Random().nextInt(5));
        item.setName("name" + new Random().nextInt(5));
        item.setFlage("flage" + new Random().nextInt(5));
        return item;
    }
}


class StreamingDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //获取数据源
        DataStreamSource<Item> text =
                //注意：并行度设置为1,我们会在后面的课程中详细讲解并行度
                env.addSource(new MyStreamingSource()).setParallelism(1);
//        DataStream<MyStreamingSource.Item> item = text.map(
//                (MapFunction<MyStreamingSource.Item, MyStreamingSource.Item>) value -> value);
        text.map(new MapFunction<Item, Map>() {
            @Override
            public Map map(Item item) throws Exception {
                Map<String, Object> map = object2Map(item);
                return map;
            }
        }).filter(new FilterFunction<Map>() {
            @Override
            public boolean filter(Map value) throws Exception {
                if (!value.isEmpty()) {
                    return true;
                } else return false;
            }
        }).keyBy(new KeySelector<Map, String>() {
            @Override
            public String getKey(Map value) throws Exception {
                System.out.println("map:"+value.toString());
                String flage = (String) value.get("flage");
                System.out.println("appId值为："+flage);
                return flage;
            }
        }).process(new DeduplicateProcessFunc());

//        SingleOutputStreamOperator<String> item = text.map(new MyMapFunction());
        //打印结果
//        item.print().setParallelism(1);
//        item.printToErr();
        String jobName = "user defined streaming source";
        env.execute(jobName);
    }


    private static class MyMapFunction extends RichMapFunction<Item,String>{

        @Override
        public String map(Item value) throws Exception {
            return value.getName();
        }
    }


    private static Map<String, Object> object2Map(Object object){
        JSONObject jsonObject = (JSONObject) JSON.toJSON(object);
        Set<Map.Entry<String,Object>> entrySet = jsonObject.entrySet();
        Map<String, Object> map=new HashMap<String,Object>();
        for (Map.Entry<String, Object> entry : entrySet) {
            map.put(entry.getKey(), entry.getValue());
        }
        return map;
    }
}

*/
