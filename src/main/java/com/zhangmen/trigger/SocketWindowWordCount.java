package com.zhangmen.trigger;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


/**
 * TODO 触发器测试
 */
public class SocketWindowWordCount {


    public static void main(String[] args) throws Exception {

        /***
         *   1:在本机服务器执行命令: nc -l 9000
         *   2: 启动该服务
         *   3: 在命令行发数据
         */

        // 创建 execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 通过连接 socket 获取输入数据，这里连接到本地9000端口，如果9000端口已被占用，请换一个端口
        DataStream<String> text = env.socketTextStream("localhost", 9999, "\n");

        // 解析数据，按 word 分组，开窗，聚合
        DataStream<Tuple2<String, Integer>> windowCounts = text
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
                        for (String word : value.split("\\s")) {
                            out.collect(Tuple2.of(word, 1));
                        }
                    }
                })
                .keyBy(0)
                .timeWindow(Time.seconds(20))
                .trigger(CustomTrigger.create())
                .sum(1);

        windowCounts.print().setParallelism(1);

        env.execute("Socket Window WordCount");
    }
}

/**
 * trigger 接口有5个方法如下：
     onElement()方法,每个元素被添加到窗口时调用
 　　onEventTime()方法,当一个已注册的事件时间计时器启动时调用
 　　onProcessingTime()方法,当一个已注册的处理时间计时器启动时调用
 　　onMerge()方法，与状态性触发器相关，当使用会话窗口时，两个触发器对应的窗口合并时，合并两个触发器的状态。
 　　*最后一个clear()方法执行任何需要清除的相应窗

    CONTINUE:什么也不做
    FIRE:触发计算
    PURGE:清除窗口中的数据
    FIRE_AND_PURGE:触发计算并清除窗口中的数据
 */
class CustomTrigger extends Trigger<Object, TimeWindow> {
    private static final long serialVersionUID = 1L;
    public CustomTrigger() {

    }

    private static int flag = 0;

    @Override
    public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx){
        ctx.registerEventTimeTimer(window.maxTimestamp());
        if (flag > 4) {
            flag = 0;
            return TriggerResult.FIRE;
        }else{
            flag ++;
        }
        System.out.println("onElement: " + element +ctx.getCurrentWatermark());
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception{
        System.out.println("111-->>");
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        System.out.println("222-->>");
        return TriggerResult.FIRE;
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception{
        System.out.println("333-->>");
        ctx.deleteProcessingTimeTimer(window.maxTimestamp());
    }

    @Override
    public String toString(){
        return "CustomTrigger";
    }

//    @Override
//    public void onMerge(TimeWindow window, OnMergeContext ctx) throws Exception {
//        long windowMaxTimestamp = window.maxTimestamp();
//        if (windowMaxTimestamp > ctx.getCurrentProcessingTime()) {
//            ctx.registerProcessingTimeTimer(windowMaxTimestamp);
//        }
//    }

    public static CustomTrigger create(){
        return new CustomTrigger();
    }
}