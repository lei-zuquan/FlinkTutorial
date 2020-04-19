package com.lei.wc


import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 11:25 上午 2020/4/19
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
object C02_StreamWordCount {
  def main(args: Array[String]): Unit = {

    // --host localhost --port 7777
    val params: ParameterTool = ParameterTool.fromArgs(args)
    val host: String = params.get("host")
    val port: Int = params.getInt("port")


    // 创建一个流处理的执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 接收socket数据流
    val textDataStream: DataStream[String] = env.socketTextStream(host, port)

    // 逐一读取数据，分词之后进行wordcount
    val wordCountDataStream: DataStream[(String, Int)] = textDataStream.flatMap(_.split("\\s"))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    // 打印输出，流处理到这里才只是定义了流处理流程
    //wordCountDataStream.print()
    wordCountDataStream.print().setParallelism(2) // 设置并行度，如果没有指定默认是电脑CPU核心数

    // 打印输出，传入job名称
    env.execute("stream word count job")

    // 启动一个socket输入
    // nc -lk 7777
  }
}
