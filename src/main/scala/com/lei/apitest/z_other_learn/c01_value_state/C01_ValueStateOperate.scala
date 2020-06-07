package com.lei.apitest.z_other_learn.c01_value_state


import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: 2020-05-20 12:43
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */


/*
    作用
      保存一个可以更新和检索的值

    需求
      通过valuestate来实现求取平均值
 */

object ValueStateOperate {

  /**
   * 程序的入口类
   * @param args
   */
  def main(args: Array[String]): Unit = {

    //程序的入口类
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //导入隐式转换的包，不然会报错的
    import org.apache.flink.api.scala._
    //从元组里面获取数据源
    env.fromCollection(List(
      (1L, 3d),
      (1L, 5d),
      (1L, 7d),
      (1L, 4d),
      (1L, 2d)
    )).keyBy(_._1)//按照每一个key进行分组
      .flatMap(new CountWindowAverage)
      .print()

    env.execute()
  }
}

/**
 * 定义输入数据类型以及输出的数据类型都是元组
 */
class  CountWindowAverage extends RichFlatMapFunction[(Long,Double),(Long,Double)]{

  private var sum: ValueState[(Long, Double)] = _

  /*
  覆写父类的open方法
  相当于初始化的方法
   */
  override def open(parameters: Configuration): Unit = {
    val valueStateDescriptor = new ValueStateDescriptor[(Long,Double)]("average",classOf[(Long,Double)])

    sum = getRuntimeContext.getState(valueStateDescriptor)
  }

  /**
   *
   * @param input  每次传入进来的数据
   * @param out
   */
  override def flatMap(input: (Long, Double), out: Collector[(Long, Double)]): Unit = {
    val tmpCurrentSum: (Long, Double) = sum.value()  //获取到状态当中的值   第一次的时候，这个值获取不到的
    val currentSum= if(tmpCurrentSum != null){
      //表示已经获取到了值
      tmpCurrentSum
    }else{
      //没取到值,默认就给一个初始值
      (0L,0d)
    }

    //每次来了一个相同key的数据，就给数据记做出现1次  ==》 数据对应的值全部都累加起来了
    val newSum = (currentSum._1 +1 ,currentSum._2  +  input._2)

    //更新状态值
    sum.update(newSum)

    //求取平均值
    if(newSum._1 >=2  ){
      //input._1  得到的是key     newSum._2/newSum._1)  ==》 对应的key，结果的平均值
      out.collect((input._1,newSum._2/newSum._1))
      // sum.clear()
    }
  }
}
