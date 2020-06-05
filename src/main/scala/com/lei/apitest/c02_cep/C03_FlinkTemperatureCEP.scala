package com.lei.apitest.c02_cep

import java.util

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time


// 定义温度信息pojo
case class DeviceDetail(sensorMac:String,deviceMac:String,temperature:String,dampness:String,pressure:String,date:String)

// 报警的设备信息样例类


case class AlarmDevice(sensorMac:String,deviceMac:String,temperature:String)

// 在三分钟之内，出现温度高于40度，三次就报警
// 传感器设备mac地址，检测机器mac地址，温度
// 传感器设备mac地址，检测机器mac地址，温度，湿度，气压，数据产生时间
// 00-34-5E-5F-89-A4,00-01-6C-06-A6-29,38,0.52,1.1,2020-03-02 12:20:32

object FlinkTemperatureCEP {

  private val format: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //指定时间类型 以事件的时间为准
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    environment.setParallelism(1)
    import org.apache.flink.api.scala._

    //接受数据
    val sourceStream: DataStream[String] = environment.socketTextStream("node-01",7777)

    val deviceStream: KeyedStream[DeviceDetail, String] = sourceStream.map(x => {
      val strings: Array[String] = x.split(",")
      DeviceDetail(strings(0), strings(1), strings(2), strings(3), strings(4), strings(5))
    }).assignAscendingTimestamps(x =>{format.parse(x.date).getTime})   //设置数据的水印 没有考虑迟到的数据
      .keyBy(x => x.sensorMac)

    //定义数据监测的pattern
    //在三分钟之内，出现温度高于40度，三次就报警
    val pattern: Pattern[DeviceDetail, DeviceDetail] = Pattern.begin[DeviceDetail]("start")
      .where(x => x.temperature.toInt >= 40)
      .followedByAny("follow")
      .where(x => x.temperature.toInt >= 40)
      .followedByAny("third")
      .where(x => x.temperature.toInt >= 40)
      .within(Time.minutes(3))

    Pattern.begin[DeviceDetail]("begin")
      .where(x => x.temperature.toInt >=40)
     // .times(3)//三分钟之内，出现三次
     // .times(2,4)  //三分钟之内，出现两次，三次，或者四次情况
      .timesOrMore(3)   //三分钟之内，出现三次及以上的情况
      .within(Time.minutes(3))

    val patternResult: PatternStream[DeviceDetail] = CEP.pattern(deviceStream,pattern)

    //获取得到的数据
    patternResult.select(new MyPatternResultFunction).print()

    environment.execute()
  }

}


// 自定义PatternSelectFunction
class MyPatternResultFunction extends PatternSelectFunction[DeviceDetail,AlarmDevice]{
  override def select(pattern: util.Map[String, util.List[DeviceDetail]]): AlarmDevice = {
    val startDetails: util.List[DeviceDetail] = pattern.get("start")
    val followDetails: util.List[DeviceDetail] = pattern.get("follow")
    val thirdDetails: util.List[DeviceDetail] = pattern.get("third")

    val startResult: DeviceDetail = startDetails.iterator().next()
    val followResult: DeviceDetail = followDetails.iterator().next()
    val thirdResult: DeviceDetail = thirdDetails.iterator().next()

    println("第一条数据: "+startResult)
    println("第二条数据: "+followResult)
    println("第三条数据: "+thirdResult)

    AlarmDevice(thirdResult.sensorMac,thirdResult.deviceMac,thirdResult.temperature)
  }
}

/*
* 场景介绍

  * 现在工厂当中有大量的传感设备，用于检测机器当中的各种指标数据，例如温度，湿度，气压等，并实时上报数据到数据中心，现在需要检测，某一个传感器上报的温度数据是否发生异常。

* 异常的定义

  * 三分钟时间内，出现三次及以上的温度高于40度就算作是异常温度，进行报警输出

* 收集数据如下：

  ~~~
  传感器设备mac地址，检测机器mac地址，温度，湿度，气压，数据产生时间

00-34-5E-5F-89-A4,00-01-6C-06-A6-29,38,0.52,1.1,2020-03-02 12:20:32
00-34-5E-5F-89-A4,00-01-6C-06-A6-29,47,0.48,1.1,2020-03-02 12:20:35
00-34-5E-5F-89-A4,00-01-6C-06-A6-29,50,0.48,1.1,2020-03-02 12:20:38
00-34-5E-5F-89-A4,00-01-6C-06-A6-29,31,0.48,1.1,2020-03-02 12:20:39
00-34-5E-5F-89-A4,00-01-6C-06-A6-29,52,0.48,1.1,2020-03-02 12:20:41
00-34-5E-5F-89-A4,00-01-6C-06-A6-29,53,0.48,1.1,2020-03-02 12:20:43
00-34-5E-5F-89-A4,00-01-6C-06-A6-29,55,0.48,1.1,2020-03-02 12:20:45
~~~
 */
