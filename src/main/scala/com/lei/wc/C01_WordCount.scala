package com.lei.wc

import org.apache.flink.api.scala._
//import org.apache.flink.api.scala.{AggregateDataSet, DataSet, ExecutionEnvironment}

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 11:11 上午 2020/4/19
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */

/**
 * Flink 入门程序 WordCount（离线）
 *
 * 对word状态进行实时统计，包含状态监控
 *
 */


// 批处理代码
object C01_WordCount {
  def main(args: Array[String]): Unit = {
    // 创建一个批处理的执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    // 从文件中读取数据
    val inputPath = "input_dir/hello.txt"
    val inputDataSet: DataSet[String] = env.readTextFile(inputPath)

    // 分词之后做count
    val wordCountDataSet: AggregateDataSet[(String, Int)] = inputDataSet.flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)

    // 打印输出
    wordCountDataSet.print()

      
  }

}
