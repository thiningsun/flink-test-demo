package com.zhangmen.checkpoint
import java.util
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * @Date 2019/7/26
  */
object CheckpointDemo {

  /**
    * 假定用户需要每隔1秒钟需要统计4秒中窗口中数据的量，然后对统计的结果值进行checkpoint处理
    *
    * 1.使用自定义算子每秒钟产生大约10000条数据。
    * 2.产生的数据为一个四元组(Long，String，String，Integer)—------(id,name,info,count)。
    * 3.数据经统计后，统计结果打印到终端输出。
    * 4.打印输出的结果为Long类型的数据。
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    /**
      *
      * 1. source算子每隔1秒钟发送10000条数据，并注入到Window算子中。
      * 2. window算子每隔1秒钟统计一次最近4秒钟内数据数量。
      * 3. 每隔1秒钟将统计结果打印到终端
      * 4. 每隔6秒钟触发一次checkpoint，然后将checkpoint的结果保存到HDFS中。
      */

    /**
      * 开发步骤
      * 1.获取执行环境
      * 2.设置检查点的环境
      * 3.创建数据
      * 4.数据转换，检查点的制作
      * 5.数据打印
      * 6.触发执行
      */
    //1.获取执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //2.检查点的设置
    env.enableCheckpointing(6000) //6秒钟
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE) //至多一次，强一致性
    env.getCheckpointConfig.setCheckpointInterval(6000)
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    env.getCheckpointConfig.setFailOnCheckpointingErrors(false)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1) //当前最大检查点数量
    //RETAIN_ON_CANCELLATION,如果取消任务，保留检查点（如果要删除HDFS上的检查点，需要手动删除）（生产上用这种）
    //DELETE_ON_CANCELLATION ,如果取消任务,删除检查点(系统会自动删除HDFS上的检查点)
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    //配置检查点路径
    env.setStateBackend(new FsStateBackend("file:///D://workspace//flink_arlen_test//data"))
    //3.创建数据
    val source: DataStream[CheckpointBean] = env.addSource(new MyCheckpoint)

    //4.设置处理时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    //5.设置水位线
    source.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[CheckpointBean] {
      override def getCurrentWatermark: Watermark = {
        new Watermark(System.currentTimeMillis())
      }

      override def extractTimestamp(element: CheckpointBean, previousElementTimestamp: Long): Long = {
        System.currentTimeMillis()
      }
    }).keyBy(_.id)
      .timeWindow(Time.seconds(4), Time.seconds(1))
      .apply(new MyCheckpointFunction)
      .print()

    //触发执行
    env.execute()
  }

}

//(Long，String，String，Integer)—------(id,name,info,count)
case class CheckpointBean(id: Long, name: String, info: String, count: Int)

class MyCheckpoint extends RichSourceFunction[CheckpointBean] {
  //source算子每隔1秒钟发送10000条数据
  override def run(ctx: SourceFunction.SourceContext[CheckpointBean]): Unit = {
    while (true) {

      for (line <- 0 until 1000) {
        ctx.collect(CheckpointBean(1, "test:" + line, "test example", 1))
      }
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = ???
}

class State extends Serializable {
  var total: Long = 0L

  def getTotal = total

  def setTotal(value: Long) = {
    total = value
  }
}

class MyCheckpointFunction extends WindowFunction[CheckpointBean, Long, Long, TimeWindow]
  with ListCheckpointed[State] {
  var total:Long = 0L
  override def apply(key: Long, window: TimeWindow, input: Iterable[CheckpointBean], out: Collector[Long]): Unit = {
    var count:Long = 0L
    for (line <- input) {
      count = count + line.count
    }
    total+=count
    out.collect(total)
  }

  override def snapshotState(checkpointId: Long, timestamp: Long): util.List[State] = {
    val states = new util.ArrayList[State]()
    val state = new State
    state.setTotal(total)
    states.add(state)
    states
  }

  override def restoreState(state: util.List[State]): Unit = {
    total = state.get(0).getTotal
  }
}
