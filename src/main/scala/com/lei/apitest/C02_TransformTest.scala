package com.lei.apitest

import org.apache.flink.api.common.functions.{FilterFunction, RichMapFunction}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, KeyedStream, SplitStream, StreamExecutionEnvironment}
/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 12:33 下午 2020/4/20
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */

/**
 * Flink流的常规操作拆分与合并
 *
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

    // 1.基本转换算子和简单聚合算子
    val streamFromFile: DataStream[String] = env.readTextFile("input_dir/sensor.txt")
    streamFromFile.print()

    val dataStream: DataStream[SensorReading] = streamFromFile.map(data => {
      val dataArray: Array[String] = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })
      .keyBy(0)
      //.sum(2)
      // 输出当前传感器最新的温度+10，而时间戳是上一次数据的时间+1
      .reduce((x, y) => SensorReading(x.id, x.timestamp + 1, y.temperature + 10))

//    val aggStream: KeyedStream[SensorReading, Tuple] = dataStream.keyBy(0)
//
//
//
//    // 如下是flink算子在使用scala时，可能导致输出类型不一致问题
//    //val value1: KeyedStream[SensorReading, Tuple] = dataStream.keyBy(0)
//    //val value2: KeyedStream[SensorReading, String] = dataStream.keyBy(_.id)
//
//    aggStream.print()
//
//    // 2.多流转换算子
//    // 分流split
    val splitStream: SplitStream[SensorReading] = dataStream.split(data => {
      if (data.temperature > 30) Seq("high") else Seq("low")
    })
//
//    val high: DataStream[SensorReading] = splitStream.select("high")
//    val low: DataStream[SensorReading] = splitStream.select("low")
//    val all: DataStream[SensorReading] = splitStream.select("high", "low")
//
//    high.print("high")
//    low.print("low")
//    all.print("all")
//
//
//    // 合并两条流
//    // connect合并两条流，两条流数据类型不一致也可以；最多2条流
//    val warningStream: DataStream[(String, Double)] = high.map(data => (data.id, data.temperature))
//    val connectedStream: ConnectedStreams[(String, Double), SensorReading] = warningStream.connect(low)
//
//    val coMapDataStream: DataStream[Product] = connectedStream.map(
//      warningData => (warningData._1, warningData._2, "warning"),
//      lowData => (lowData.id, "healthy")
//    )
//    coMapDataStream.print()
//
//    // union合并流
//    // 多条流数据类型要求一致，数据结构必须一样
//    val unionStream: DataStream[SensorReading] = high.union(low)
//    unionStream.print("union")

    /**
     * Connect与Union区别
     * 1、Union之前两个流的类型必须是一样， Connect可以不一样，在之后的coMap中再去调整成为一样的。
     * 2、Connect只能操作两个流，Union可以操作多个。
     */

    // 函数类
    dataStream.filter(new MyFilter()).print()


    env.execute("transform test")

  }

}

class MyFilter extends FilterFunction[SensorReading] {
  override def filter(value: SensorReading): Boolean = {
    value.id.startsWith("sensor_1")
  }
}

// 富函数RichMapFunction
class MyMapper() extends RichMapFunction[SensorReading, String] {
  override def map(in: SensorReading): String = {
    "flink"
  }
}
