package com.lei.apitest

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 7:45 下午 2020/4/21
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */

/*
      连续数据中如果温度上升，我们判定为有异常，进行报警提示

 */
object C04_ProcessFunctionTest {
  def main(args: Array[String]): Unit = {


    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 默认时间语义是：processes time
    //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // watermark产生的事件间隔(每n毫秒)是通过ExecutionConfig.setAutoWatermarkInterval(...)来定义的
    //env.getConfig.setAutoWatermarkInterval(100L) // 默认200毫秒

    // source
    val inputStream: DataStream[String] = env.socketTextStream("localhost", 7777)

    // Transform操作
    val dataStream: DataStream[SensorReading] = inputStream.map(data => {
      val dataArray: Array[String] = data.split(",")
      // 转成String 方便序列化输出
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })

    val processedStream: DataStream[String] = dataStream.keyBy(_.id)
      .process(new TempIncreAlert())

    dataStream.print("input data")

    processedStream.print("processedStream:")

    env.execute("window test")
      
  }

}


class TempIncreAlert() extends KeyedProcessFunction[String, SensorReading, String]{

  // 定义一个状态，用来保存上一个数据的温度值。将之前的数据保存到状态里
  lazy val lastTemp: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))
  // 定义一个状态，用来保存定时器的时间戳
  lazy val currentTimer: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("currentTimer", classOf[Long]))


  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, collector: Collector[String]): Unit = {
    // 先取出上一个温度值
    val preTemp = lastTemp.value()
    // 更新温度值
    lastTemp.update(value.temperature)

    val curTimerTs: Long = currentTimer.value()

    // 温度上升且没有设过定时器，则注册定时器
    if (preTemp > value.temperature || preTemp == 0.0) {
      // 如果温度下降，或是第一条数据，删除定时器并清空状态
      ctx.timerService().deleteProcessingTimeTimer(curTimerTs)
      currentTimer.clear()
    } else if (value.temperature > preTemp && curTimerTs == 0){
      val timerTs: Long = ctx.timerService().currentProcessingTime() + 10000L
      ctx.timerService().registerProcessingTimeTimer(timerTs)
      currentTimer.update(timerTs)
    }
  }


  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    //super.onTimer(timestamp, ctx, out)
    // 输出报警信息
    out.collect(ctx.getCurrentKey + " 温度连续上升")
    currentTimer.clear()
  }
}