package com.lei.apitest.value_state


import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import scala.collection.mutable.ListBuffer


/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: 2020-05-20 12:41
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */

/*
  需求
    实现每两条数据进行输出打印一次，不用区分数据的key
    这里使用ListState实现
 */
/**
 * 实现每两条数据进行输出打印一次，不用区分数据的key
 */
object OperatorListState {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    //构造数据的输入流
    val sourceStream: DataStream[(String, Int)] = env.fromCollection(List(
      ("spark", 3),
      ("hadoop", 5),
      ("hive", 7),
      ("flume", 9)
    ))

    //
    //每隔两条数据   ==》 打印一下
    sourceStream.addSink(new OperateTaskState).setParallelism(1)
    env.execute()
  }

}

class OperateTaskState extends SinkFunction[(String,Int)]{
  //定义一个list 用于我们每两条数据打印一下
  private var listBuffer:ListBuffer[(String,Int)] = new ListBuffer[(String, Int)]

  override def invoke(value: (String, Int), context: SinkFunction.Context[_]): Unit = {
    listBuffer.+=(value)

    if(listBuffer.size ==2){
      println(listBuffer)

      //清空state状态   每隔两条数据，打印一下之后，清空状态
      listBuffer.clear()
    }
  }

}
