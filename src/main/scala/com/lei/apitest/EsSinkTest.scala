package com.lei.apitest

import com.lei.util.MyEsUtil
import org.apache.flink.streaming.api.scala._

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 11:08 下午 2020/4/20
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
object EsSinkTest {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // source
    val inputStream: DataStream[String] = env.readTextFile("input_dir/sensor.txt")

    // Transform操作
    val dataStream: DataStream[SensorReading] = inputStream.map(data => {
      val dataArray: Array[String] = data.split(",")
      // 转成String 方便序列化输出
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })

    // sink
    dataStream.addSink(MyEsUtil.getElasticSearchSink("sensor"))

    env.execute("es sink test")
  }
}
