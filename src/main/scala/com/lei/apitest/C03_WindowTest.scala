package com.lei.apitest

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks, KeyedProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 2:44 下午 2020/4/21
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
object C03_WindowTest {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 默认时间语义是：processes time
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // watermark产生的事件间隔(每n毫秒)是通过ExecutionConfig.setAutoWatermarkInterval(...)来定义的
    env.getConfig.setAutoWatermarkInterval(100L) // 默认200毫秒

    // source
    // val inputStream: DataStream[String] = env.readTextFile("input_dir/sensor.txt")
    val inputStream: DataStream[String] = env.socketTextStream("localhost", 7777)

    // Transform操作
    val dataStream: DataStream[SensorReading] = inputStream.map(data => {
      val dataArray: Array[String] = data.split(",")
      // 转成String 方便序列化输出
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })
//      .assignAscendingTimestamps(_.timestamp * 1000)
//      .assignTimestampsAndWatermarks(new MyAssigner())
      // 经典写法是：BoundedOutOfOrdernessTimestampExtractor

      /*
       延迟一秒钟上涨水位，这样的操作; 比如：timeWindow是10秒，WaterMark设置成1秒，就是当11秒的数据过来时，才会触发窗口结束
       然后一次会继续上一次的水位时间封装消费10秒窗口数据，同时也得满足水位线延迟1秒
       */
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(element: SensorReading): Long = {
          element.timestamp * 1000
        }
      })

    val minTempPerWindowStream: DataStream[(String, Double)] = dataStream
      .map(data => (data.id, data.temperature))
      .keyBy(_._1)
      .timeWindow(Time.seconds(10)) // 开时间窗口 // 统计10秒内的最小温度
      //.timeWindow(Time.seconds(15), Time.seconds(5)) // 统计15秒内的最小温度，隔5秒输出一次；左闭右开【）包含开始，不包含结束
      // timeWindow底层调用的还是window
      //.window(SlidingEventTimeWindows.of(Time.seconds(15), Time.seconds(5), Time.hours(-8)))

      .reduce((data1, data2) => (data1._1, data1._2.min(data2._2))) // 用reduce做增量聚合

    minTempPerWindowStream.print("min temp")

    dataStream.print("input data")

    dataStream.keyBy(_.id)
        .process(new MyProcess())

    env.execute("window test")

  }
}

// 周期性生成的
//class MyAssigner() extends AssignerWithPeriodicWatermarks[SensorReading]{
//
//  val bound = 60000
//  var maxTs = Long.MinValue
//
//  override def getCurrentWatermark: Watermark = {
//    new Watermark(maxTs - bound)
//  }
//
//  override def extractTimestamp(element: SensorReading, l: Long): Long = {
//    val maxTs: Long = maxTs.max(element.timestamp * 1000)
//    element.timestamp * 1000
//  }
//}

class MyAssigner() extends AssignerWithPunctuatedWatermarks[SensorReading] {
  override def checkAndGetNextWatermark(lastElement: SensorReading, extractedTimestamp: Long): Watermark = {
    new Watermark(extractedTimestamp)
  }

  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
    element.timestamp * 1000
  }
}

class MyProcess() extends KeyedProcessFunction[String, SensorReading, String]{
  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, collector: Collector[String]): Unit = {
    ctx.timerService().registerEventTimeTimer(2000)
  }
}