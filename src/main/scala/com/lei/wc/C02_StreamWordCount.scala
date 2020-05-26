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

/**
 * Flink 入门程序 WordCount（实时）
 *
 * 对word状态进行实时统计，包含状态监控
 *
 */

object C02_StreamWordCount {
  def main(args: Array[String]): Unit = {
    /*
        注意：需要在服务器对环境变量新增HADOOP_CONF_DIR路径，具体如下：
            1. vi /etc/profile
            2. 添加：export HADOOP_CONF_DIR=/etc/hadoop/conf
     */

    // 启动Flink集群：/usr/local/flink_learn/flink-1.7.2/bin/start-cluster.sh
    // 使用WebUI查看Flink集群启动情况：http://node-01:8081/#/overview

    // --host localhost --port 7777
    // standalone提交方式：（含后台运行）
    // ./bin/flink run -c com.lei.wc.C02_StreamWordCount -p 2 /usr/local/spark-study/FlinkTutorial-1.0.jar --host localhost --port 7777
    // ./bin/flink run -c com.lei.wc.C02_StreamWordCount -p 2 /usr/local/spark-study/FlinkTutorial-1.0-jar-with-dependencies.jar --host localhost --port 7777

    // 列出正在运行的flink作业:
    // ./bin/flink list
    // ./bin/flink cancel xxxx_id

    // 查看所有flink作业
    // ./bin/flink list --all
    val params: ParameterTool = ParameterTool.fromArgs(args)
    val host: String = params.get("host")
    val port: Int = params.getInt("port")


    // 创建一个流处理的执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //env.setParallelism(1)
    //env.disableOperatorChaining() // 禁用任务链划分

    // 接收socket数据流
    val textDataStream: DataStream[String] = env.socketTextStream(host, port)

    // 逐一读取数据，分词之后进行wordcount
    val wordCountDataStream: DataStream[(String, Int)] = textDataStream.flatMap(_.split("\\s"))
      .filter(_.nonEmpty)
      //.filter(_.nonEmpty).disableChaining() // 禁用任务链划分
      //.filter(_.nonEmpty).startNewChain()     // 开始新的任务链
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    // 打印输出，流处理到这里才只是定义了流处理流程
    //wordCountDataStream.print()
    wordCountDataStream.print().setParallelism(1) // 设置并行度，如果没有指定默认是电脑CPU核心数

    // 打印输出，传入job名称
    env.execute("stream word count job")

    // 启动一个socket输入
    // nc -lk 7777
  }
}
