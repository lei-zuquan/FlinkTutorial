package com.lei.apitest

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 10:32 下午 2020/4/19
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */

// 将数据包装成样例类
// 温度传感器读数样例类（传感器id, 时间戳, 温度）
case class SensorReading(id: String, timestamp: Long, temperature: Double)

object SourceTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 1、从自定义的集合中读取数据
    val stream1: DataStream[SensorReading] = env.fromCollection(List(
      SensorReading("sensor_1", 1547718199, 35.80018327300259),
      SensorReading("sensor_6", 1547718201, 18.402894393403084),
      SensorReading("sensor_7", 1547718202, 6.720945201171228),
      SensorReading("sensor_10", 1547718205, 38.101067604893444)
    ))
    //stream1.print("stream1").setParallelism(1)

    // 2、从文件中读取数据
    val stream2: DataStream[String] = env.readTextFile("input_dir/sensor.txt")
    stream2.print("stream1").setParallelism(1)

    // 3、从Element中读取数据
    env.fromElements(1, 2.0, "String").print()

    env.execute("source test")

  }
}
