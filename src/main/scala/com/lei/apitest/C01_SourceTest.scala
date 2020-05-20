package com.lei.apitest

import com.lei.util.{MyKafkaUtil}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import scala.util.Random
/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 10:32 下午 2020/4/19
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */

/**
 * Flink自定义Source:
 *    1.从定义的集合中读取数据
 *    2.从文件中读取数据
 *    3.从Element中读取数据
 *    4.从Kafka中读取数据
 *    5.自定义Source
 *
 */
// 生产环境中：将数据包装成样例类
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
    //stream2.print("stream1").setParallelism(1)

    // 3、从Element中读取数据
    env.fromElements(1, 2.0, "String").print()

    // 4、从Kafka中读取数据
    val stream3: DataStream[String] = env.addSource(MyKafkaUtil.getConsumer("GMALL_STARTUP"))
    stream3.print("stream3").setParallelism(1)

    // 5、自定义source
//    val stream4: DataStream[SensorReading] = env.addSource(new SensorSource())
//    stream4.print("stream4").setParallelism(1)

    env.execute("source test")

  }
}


class SensorSource()extends SourceFunction[SensorReading]{

  // 定义一个flag，表示数据源是否正常运行
  var running: Boolean = true

  // 取消数据源的生成
  override def cancel(): Unit = {
    running = false
  }

  // 正常生成数据
  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
    // 初始化一个随机数发生器
    val random = new Random()

    // 隔一段时间，传感器温度发生变化
    // 初始化定义一组传感器温度数据
    var curTemp = 1.to(10).map(
      // 下一个高斯分布值，正态分布
      i => ("sensor_" + i, 60 + random.nextGaussian() * 20)
    )

    // 用无限循环，产生数据流
    while (running){
      // 在前一次温度的基础上更新温度值
      curTemp = curTemp.map(
        t => (t._1, t._2 + random.nextGaussian())
      )

      // 获取当前时间戳
      val curTime: Long = System.currentTimeMillis()
      curTemp.foreach(
        t => sourceContext.collect(SensorReading(t._1, curTime, t._2))
      )

      Thread.sleep(1000)
    }
  }


}