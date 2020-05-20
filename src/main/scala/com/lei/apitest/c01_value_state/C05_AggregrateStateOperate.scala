package com.lei.apitest.c01_value_state

import org.apache.flink.api.common.functions.{AggregateFunction, RichFlatMapFunction}
import org.apache.flink.api.common.state.{AggregatingState, AggregatingStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector
/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: 2020-05-20 12:37
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */

/*
  作用
    将相同key的数据进行聚合
  需求
    将相同key的数据聚合成为一个字符串
 */
/**
 * 将相同key的数据聚合成为一个字符串
 */
object AggregrateStateOperate {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    env.fromCollection(List(
      (1L, 3d),
      (1L, 5d),
      (1L, 7d),
      (2L, 4d),
      (2L, 2d),
      (2L, 6d)
    )).keyBy(_._1)
      .flatMap(new AggregrageState)
      .print()

    env.execute()
  }
}

/**
 *   (1L, 3d),
        (1L, 5d),
        (1L, 7d),   把相同key的value拼接字符串：Contains+and+3+and+5+and+7
 */
class AggregrageState extends RichFlatMapFunction[(Long,Double),(Long,String)]{

  //定义AggregatingState
  private var aggregateTotal:AggregatingState[Double, String] = _

  override def open(parameters: Configuration): Unit = {
    /**
     * name: String,
     * aggFunction: AggregateFunction[IN, ACC, OUT],
     * stateType: Class[ACC]
     */
    val aggregateStateDescriptor = new AggregatingStateDescriptor[Double, String, String]("aggregateState", new AggregateFunction[Double, String, String] {
      //创建一个初始值
      override def createAccumulator(): String = {
        "Contains"
      }

      //对数据进行累加
      override def add(value: Double, accumulator: String): String = {
        if ("Contains".equals(accumulator)) {
          accumulator + value
        }

        accumulator + "and" + value
      }

      //获取累加的结果
      override def getResult(accumulator: String): String = {
        accumulator
      }

      //数据合并的规则
      override def merge(a: String, b: String): String = {
        a + "and" + b
      }
    }, classOf[String])
    aggregateTotal = getRuntimeContext.getAggregatingState(aggregateStateDescriptor)
  }

  override def flatMap(input: (Long, Double), out: Collector[(Long, String)]): Unit = {
    aggregateTotal.add(input._2)
    out.collect(input._1,aggregateTotal.get())
  }
}