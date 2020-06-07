package com.lei.apitest.z_other_learn.c02_cep

import java.util

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time


case class OrderDetail(orderId:String,status:String,orderCreateTime:String,price :Double)


object OrderTimeOutCheckCEP {

  private val format: FastDateFormat = FastDateFormat.getInstance("yyy-MM-dd HH:mm:ss")

  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    environment.setParallelism(1)
    import org.apache.flink.api.scala._
    val sourceStream: DataStream[String] = environment.socketTextStream("node-01",7777)

    val keyedStream: KeyedStream[OrderDetail, String] = sourceStream.map(x => {
      val strings: Array[String] = x.split(",")
      OrderDetail(strings(0), strings(1), strings(2), strings(3).toDouble)
    // 给数据添加水印，解决乱序的问题
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderDetail](Time.seconds(5)){
      override def extractTimestamp(element: OrderDetail): Long = {
        format.parse(element.orderCreateTime).getTime
      }
    }).keyBy(x => x.orderId)

    // 定义Pattern模式，指定条件
    val pattern: Pattern[OrderDetail, OrderDetail] = Pattern.begin[OrderDetail]("start")
      .where(order => order.status.equals("1"))  //订单的最开始的状态是1
      .followedBy("second")    //第二个条件  订单的状态是2   严格临近模式
      .where(x => x.status.equals("2"))
      .within(Time.minutes(15))   //在15分钟之内出现2 即可

    // 4. 调用select方法，提取事件序列，超时的事件要做报警提示
    val orderTimeoutOutputTag = new OutputTag[OrderDetail]("orderTimeout")

    val patternStream: PatternStream[OrderDetail] = CEP.pattern(keyedStream,pattern)
    val selectResultStream: DataStream[OrderDetail] = patternStream
      .select(orderTimeoutOutputTag, new OrderTimeoutPatternFunction, new OrderPatternFunction)

    selectResultStream.print()
    // 打印侧输出流数据 过了15分钟还没支付的数据
    selectResultStream.getSideOutput(orderTimeoutOutputTag).print()
    environment.execute()
  }
}

// 订单超时检测
class OrderTimeoutPatternFunction extends PatternTimeoutFunction[OrderDetail,OrderDetail]{

  override def timeout(pattern: util.Map[String, util.List[OrderDetail]], l: Long): OrderDetail = {
    val detail: OrderDetail = pattern.get("start").iterator().next()
    println("超时订单号为" + detail)
    detail
  }
}

class OrderPatternFunction extends PatternSelectFunction[OrderDetail,OrderDetail] {
  override def select(pattern: util.Map[String, util.List[OrderDetail]]): OrderDetail = {
    val detail: OrderDetail = pattern.get("second").iterator().next()
    println("支付成功的订单为" + detail)
    detail
  }
}


/*
* 场景介绍

  * 在我们的电商系统当中，经常会发现有些订单下单之后没有支付，就会有一个倒计时的时间值，提示你在15分钟之内完成支付，如果没有完成支付，那么该订单就会被取消，主要是因为拍下订单就会减库存，但是如果一直没有支付，那么就会造成库存没有了，别人购买的时候买不到，然后别人一直不支付，就会产生有些人买不到，有些人买到了不付款，最后导致商家一件产品都卖不出去

* 需求

  * 创建订单之后15分钟之内一定要付款，否则就取消订单

* 订单数据格式如下类型字段说明
  * 订单编号

  * 订单状态

    * 1.创建订单,等待支付
    * 2.支付订单完成
    * 3.取消订单，申请退款
    * 4.已发货
    * 5.确认收货，已经完成

  * 订单创建时间

  * 订单金额

    ~~~
    20160728001511050311389390,1,2016-07-28 00:15:11,295
    20160801000227050311955990,1,2016-07-28 00:16:12,165
    20160728001511050311389390,2,2016-07-28 00:18:11,295
    20160801000227050311955990,2,2016-07-28 00:18:12,165
    20160728001511050311389390,3,2016-07-29 08:06:11,295
    20160801000227050311955990,4,2016-07-29 12:21:12,165
    20160804114043050311618457,1,2016-07-30 00:16:15,132
    20160801000227050311955990,5,2016-07-30 18:13:24,165
    ~~~


* 规则，出现 1 创建订单标识之后，紧接着需要在15分钟之内出现 2 支付订单操作，中间允许有其他操作
 */