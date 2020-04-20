package com.lei.apitest

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 12:33 下午 2020/4/20
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
object TransformTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1) // 设置全局的并行度

    // map
    // flatMap
    // filter
    // keyBy: 逻辑地将一个流拆分成不相交的分区，每个分区包含具有相同key的元素
    //      DataStream -> KeyedSteam(内部以Hash的形式实现的，可能数据倾斜)
    // 滚动聚合算子（Rolling Aggregation）
    //      sum()
    //      min()
    //      max()
    //      minBy()
    //      maxBy()
    val streamFromFile: DataStream[String] = env.readTextFile("input_dir/sensor.txt")
    streamFromFile.print()
                                             //env.readTextFile("input_dir/sensor.txt")
    val dataStream: DataStream[SensorReading] = streamFromFile.map(data => {
      val dataArray: Array[String] = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })
      .keyBy(0)
      .sum(2)

    dataStream.print()

    env.execute("transform test")

  }

}
