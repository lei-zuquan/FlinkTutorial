package com.lei.apitest.c02_cep

import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.pattern.conditions.IterativeCondition
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * 在三分钟，一旦发生ip变化，马上检测到
  */
case class UserLoginInfo(ip:String,username:String,operateUrl:String,time:String)


object CheckIpChangeWithCEP {
  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    //todo:1、接受socket数据源
    val sourceStream: DataStream[String] = environment.socketTextStream("node01",9999)

    val keyedStream: KeyedStream[(String, UserLoginInfo), String] = sourceStream.map(x => {
      val strings: Array[String] = x.split(",")
      (strings(1), UserLoginInfo(strings(0), strings(1), strings(2), strings(3)))
    }).keyBy(x => x._1)
    // 按照用户名进行分组

    // 定义CEP当中的pattern
    val pattern: Pattern[(String, UserLoginInfo), (String, UserLoginInfo)] =
      Pattern.begin[(String, UserLoginInfo)]("start") //使用begin定义第一个条件
      .where(x => x._2.username != null)
      .next("second") //使用next来定义第二个条件
      .where(new IterativeCondition[(String, UserLoginInfo)] {
      /**
        *
        * @param value   传入的数据值
        * @param context 上下文对象  ==》可以获取满足第一个条件的数据
        * @return
        */
      override def filter(value: (String, UserLoginInfo),
                          context: IterativeCondition.Context[(String, UserLoginInfo)]): Boolean = {
        var flag: Boolean = false
        val firstValues: util.Iterator[(String, UserLoginInfo)]
        = context.getEventsForPattern("start").iterator()

        //满足第一个条件

        //第一个条件与第二个条件进行比较判断

        while (firstValues.hasNext) {
          val tuple: (String, UserLoginInfo) = firstValues.next()

          if (!tuple._2.ip.equals(value._2.ip)) {
            flag = true
          }
        }

        flag //如果flag是true，表示满足我们的条件的数据 两个ip不相等，ip发生了变换
      }
    }).within(Time.seconds(120))

    //将规则应用到流数据上面去
    val patternStream: PatternStream[(String, UserLoginInfo)] = CEP.pattern(keyedStream,pattern)

    //从patternStream里面获取到满足条件的数据
    patternStream.select(new MyPatternSelectFunction).print()

    environment.execute()
  }
}


//自定义PatternSelectFunction类
class MyPatternSelectFunction extends PatternSelectFunction[(String,UserLoginInfo),(String,UserLoginInfo)]{
  override def select(map: util.Map[String, util.List[(String, UserLoginInfo)]]): (String, UserLoginInfo) = {
    // 获取Pattern名称为start的事件
    val startIterator= map.get("start").iterator()

    if(startIterator.hasNext){
      println("满足start模式中的数据："+startIterator.next())
    }

    //获取Pattern名称为second的事件
    val secondIterator = map.get("second").iterator()

    var tuple:(String,UserLoginInfo)=null

    if(secondIterator.hasNext){
      tuple=secondIterator.next()
      println("满足second模式中的数据："+ tuple)
    }

    tuple
  }
}









