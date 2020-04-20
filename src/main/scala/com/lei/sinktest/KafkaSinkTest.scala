package com.lei.sinktest

import com.lei.apitest.SensorReading
import com.lei.util.MyKafkaUtil
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 8:57 下午 2020/4/20
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
object KafkaSinkTest {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // source
    //val inputStream: DataStream[String] = env.readTextFile("input_dir/sensor.txt")

    val inputStream: DataStream[String] = env.addSource(MyKafkaUtil.getConsumer("sensor"))

    // Transform操作
    val dataStream: DataStream[String] = inputStream.map(data => {
      val dataArray: Array[String] = data.split(",")
      // 转成String 方便序列化输出
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble).toString
    })

    // sink
    // dataStream.addSink(new FlinkKafkaProducer011[String]("node-01:9092", "gmall", new SimpleStringSchema()))
    dataStream.addSink(MyKafkaUtil.getProducer("GMALL_STARTUP"))
    dataStream.print()

    env.execute("kafka sink test")
  }
}
