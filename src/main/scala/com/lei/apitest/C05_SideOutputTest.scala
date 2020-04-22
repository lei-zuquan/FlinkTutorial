package com.lei.apitest

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 7:41 上午 2020/4/22
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */

/*
    侧输出流
    低温流，将温度低于XXX度的数据转移至低温流。
    低于32华氏摄氏度

    测试结果如下：
    processed data> SensorReading(sensor_1,1547718199,35.0)
    processed data> SensorReading(sensor_1,1547718199,35.0)
    frezing alert for sensor_1
    frezing alert for sensor_1
    processed data> SensorReading(sensor_1,1547718199,55.0)
    frezing alert for sensor_6

 */

/*
    Emitting to Side Outputs（侧输出）

        大部分的DataStream API的算子的输出是单一输出，也就是某种数据类型的流。
        除了split算子，可以将一条流分成多条流，这些流的数据类型也都相同。process
        function 的 side outputs 功能可以产生多条流，并且这些流的数据类型可以不一样。
        一个side output可以定义为OutputTag[X]对象，X是输出流的数据类型。process
        function可以通过Context对象发射一个事件到一个或者多个side outputs。
 */
object C05_SideOutputTest {
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

    val processedStream: DataStream[SensorReading] = dataStream
      .process(new FreezingAlert())


    //dataStream.print("input data")
    processedStream.print("processed data") // 不是打印侧输出流，而是侧输出流之后的主流
    processedStream.getSideOutput(new OutputTag[String]("freezing alert")).print() // 打印侧出流

    env.execute("window test")

  }
}

// 冰点报警，如果小于32F，输出报警信息到侧输出流
class FreezingAlert() extends ProcessFunction[SensorReading,SensorReading] {

  lazy val alertOutPut: OutputTag[String] = new OutputTag[String]("freezing alert")

  override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
    if (value.temperature < 32.0){
      ctx.output(alertOutPut, "frezing alert for " + value.id) // 侧输出流
    } else {
      out.collect(value) // 主流
    }
  }
}