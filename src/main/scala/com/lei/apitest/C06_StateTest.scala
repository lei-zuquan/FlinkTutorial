package com.lei.apitest

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
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

/**
      连续数据中如果温度上升超过指定温度差，我们判定为有异常，进行报警提示

 */

/*
      温度传感器中，连续两个温度差值超过了10度，则判定为异常，以下就是测试数据及结果
          input data> SensorReading(sensor_1,1547718199,35.0)
          flatMap差值超过阈值：> (sensor_1,0.0,35.0)
          差值超过阈值：> (sensor_1,0.0,35.0)
          input data> SensorReading(sensor_6,1547718201,18.0)
          差值超过阈值：> (sensor_6,0.0,18.0)
          flatMap差值超过阈值：> (sensor_6,0.0,18.0)
          input data> SensorReading(sensor_1,1547718199,44.0)
          input data> SensorReading(sensor_6,1547718201,28.0)
          input data> SensorReading(sensor_1,1547718199,55.0)
          差值超过阈值：> (sensor_1,44.0,55.0)
          flatMap差值超过阈值：> (sensor_1,44.0,55.0)
 */
object C06_StateTest {
  def main(args: Array[String]): Unit = {


    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 默认时间语义是：processes time
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
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

      /*
       延迟一秒钟上涨水位，这样的操作; 比如：timeWindow是10秒，WaterMark设置成1秒，就是当11秒的数据过来时，才会触发窗口结束
       然后一次会继续上一次的水位时间封装消费10秒窗口数据，同时也得满足水位线延迟1秒
       */
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(element: SensorReading): Long = {
          element.timestamp * 1000
        }
      })

    dataStream.print("input data")

    /* 温度连续上升报警
    val processedStream: DataStream[String] = dataStream.keyBy(_.id)
      .process(new TempIncreAlert06())
    processedStream.print("温度连续上升报警processedStream:")
     */

    /* 状态编程方式一：检测某个传感器温度差值不能超过一定限度，需要按传感器id进行分组
    val processedTempChangeAlertStream: DataStream[(String, Double, Double)] = dataStream.keyBy(_.id)
      .process(new TempChangeAlert(10.0))
    processedTempChangeAlertStream.print("差值超过阈值：")
     */

    /* 状态编程方式二：通过flatMap
    val flatMapChangeAlertStream: DataStream[(String, Double, Double)] = dataStream.keyBy(_.id)
      .flatMap(new TempChangeAlertFlatMap(10.0))
    flatMapChangeAlertStream.print("flatMap差值超过阈值：")
     */

    /* 状态编程方式三：通过flatMapWithState，这是flatMap实现的简化版 */
    val processedFlatMapWithState: DataStream[(String, Double, Double)] = dataStream.keyBy(_.id)
      .flatMapWithState[(String, Double, Double), Double] {
        // 如果没有状态的话，也就是没有数据来过，那么就将当前数据温度值存入状态
        case (input: SensorReading, None) => (List.empty, Some(input.temperature))
        // 如果有状态，就应该与上次的温度值比较差值，如果大于阈值就输出报警
        case (input: SensorReading, lastTemp: Some[Double]) =>
          val diff = (input.temperature - lastTemp.get).abs
          if (diff > 10.0) {
            (List((input.id, lastTemp.get, input.temperature)), Some(input.temperature))
          } else {
            (List.empty, Some(input.temperature))
          }
      }
    processedFlatMapWithState.printToErr("flatMapWithState 前后温度差值超过阈值 温度上升异常：")



    env.execute("window test")
  }
}


private class TempIncreAlert06() extends KeyedProcessFunction[String, SensorReading, String]{

  // 定义一个状态，用来保存上一个数据的温度值。将之前的数据保存到状态里
  lazy val lastTemp: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))
  // 定义一个状态，用来保存定时器的时间戳
  lazy val currentTimer: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("currentTimer", classOf[Long]))


  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {
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
      // 输出报警信息
      out.collect(ctx.getCurrentKey + " 温度连续上升")
      //val timerTs: Long = ctx.timerService().currentProcessingTime() + 100L
      //ctx.timerService().registerProcessingTimeTimer(timerTs)
      //currentTimer.update(timerTs)
    }
  }


  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    //super.onTimer(timestamp, ctx, out)
    // 输出报警信息
    out.collect(ctx.getCurrentKey + " 温度连续上升")
    currentTimer.clear()
  }
}


class TempChangeAlert(threshold: Double) extends KeyedProcessFunction[String, SensorReading, (String, Double, Double)] {

  // 定义一个状态变量，保存上次的温度值
  lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))

  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, (String, Double, Double)]#Context, out: Collector[(String, Double, Double)]): Unit = {
    // 获取上次的温度值
    val lastTemp: Double = lastTempState.value()
    if (lastTemp == 0.0) {
      lastTempState.update(value.temperature) // 将当前温度值更新至状态变量中进行保存
      return
    }
    // 用当前的温度值和上次的求差，如果大于阈值，输出报警信息
    val diff: Double = (value.temperature - lastTemp).abs
    if (diff > threshold) {
      out.collect(value.id, lastTemp, value.temperature)
    }
    lastTempState.update(value.temperature) // 将当前温度值更新至状态变量中进行保存
  }
}


class TempChangeAlertFlatMap(threshold: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)] {

  private var lastTempState: ValueState[Double] = _

  override def open(parameters: Configuration): Unit = {
    // 初始化的时候声明state变量
    lastTempState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))
  }

  override def flatMap(value: SensorReading, out: Collector[(String, Double, Double)]): Unit = {
    // 获取上次的温度值
    val lastTemp: Double = lastTempState.value()
    // 用当前的温度值和上次的求差，如果大于阈值，输出报警信息
    val diff: Double = (value.temperature - lastTemp).abs
    if (diff > threshold) {
      out.collect(value.id, lastTemp, value.temperature)
    }
    lastTempState.update(value.temperature) // 将当前温度值更新至状态变量中进行保存
  }
}
