package com.lei.apitest.doit

import org.apache.flink.streaming.api.scala._

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: 2020-06-11 14:17
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */

// scala版本的State状态存储计算

object C01_MapWithState {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val lines = env.socketTextStream("node-01", 7777)

    val keyed = lines.flatMap(_.split(" ")).map(word => (word, 1)).keyBy(0)

    val summed = keyed.mapWithState((input: (String, Int), state: Option[Int]) => {
      state match {
        case Some(count) => {
          val key = input._1
          val value = input._2
          val total = count + value
          ((key, total), Some(total))
        }
        case None => {
          (input, Some(input._2))
        }
      }
    })

    summed.print()

    env.execute()
  }

}
