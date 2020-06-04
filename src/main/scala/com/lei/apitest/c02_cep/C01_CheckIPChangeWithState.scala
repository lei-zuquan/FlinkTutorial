package com.lei.apitest.c02_cep

import java.util
import java.util.Collections

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

/**
  * 使用state编程进行代码实现进行ip检测
  */

case class UserLogin(ip:String,username:String,operateUrl:String,time:String)

object CheckIPChangeWithState {

  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    //todo:1、接受socket数据源
    val sourceStream: DataStream[String] = environment.socketTextStream("node-01",9999)

    //todo:2、数据处理
    sourceStream.map(x => {
        val strings: Array[String] = x.split(",")
        //将数据封装成为了一个元组  key：userName   value：caseClass
        (strings(1),UserLogin(strings(0),strings(1),strings(2),strings(3)))
      }
    )
      .keyBy(x => x._1)
      .process(new LoginCheckProcessFunction)
      .print()

    environment.execute("checkIpChange")
  }
}
//自定义KeyedProcessFunction类
class LoginCheckProcessFunction extends KeyedProcessFunction[String,(String,UserLogin),(String,UserLogin)]{

  //定义ListState
  var listState: ListState[UserLogin] = _

  override def open(parameters: Configuration): Unit = {
    listState = getRuntimeContext.getListState(new ListStateDescriptor[UserLogin]("changeIp",classOf[UserLogin]))
  }

  // 解析用户访问信息
  override def processElement(value: (String, UserLogin), ctx: KeyedProcessFunction[String, (String, UserLogin), (String, UserLogin)]#Context, out: Collector[(String, UserLogin)]): Unit = {
    val logins = new util.ArrayList[UserLogin]()

    // 添加到list集合
    listState.add(value._2)

    import scala.collection.JavaConverters._
    val toList: List[UserLogin] = listState.get().asScala.toList
    // 排序
    val sortList: List[UserLogin] = toList.sortBy(_.time)

    if(sortList.size ==2){
      val first: UserLogin = sortList(0)
      val second: UserLogin = sortList(1)

      if(!first.ip.equals(second.ip)){
        println("小伙子你的IP变了，赶紧回去重新登录一下")
      }

      // 移除第一个ip，保留第二个ip
      logins.removeAll(Collections.EMPTY_LIST)
      // list集合当中每次都只保存了最后一条数据
      logins.add(second)
      listState.update(logins)

    }

    out.collect(value)
  }

}

/*
 * 从socket当中输入数据源

  ~~~
  192.168.52.100,zhubajie,https://icbc.com.cn/login.html,2020-02-12 12:23:45
  192.168.54.172,tangseng,https://icbc.com.cn/login.html,2020-02-12 12:23:46
  192.168.145.77,sunwukong,https://icbc.com.cn/login.html,2020-02-12 12:23:47
  192.168.52.100,zhubajie,https://icbc.com.cn/transfer.html,2020-02-12 12:23:47
  192.168.54.172,tangseng,https://icbc.com.cn/transfer.html,2020-02-12 12:23:48
  192.168.145.77,sunwukong,https://icbc.com.cn/transfer.html,2020-02-12 12:23:49
  192.168.145.77,sunwukong,https://icbc.com.cn/save.html,2020-02-12 12:23:52
  192.168.52.100,zhubajie,https://icbc.com.cn/save.html,2020-02-12 12:23:53
  192.168.54.172,tangseng,https://icbc.com.cn/save.html,2020-02-12 12:23:54
  192.168.54.172,tangseng,https://icbc.com.cn/buy.html,2020-02-12 12:23:57
  192.168.145.77,sunwukong,https://icbc.com.cn/buy.html,2020-02-12 12:23:58
  192.168.52.100,zhubajie,https://icbc.com.cn/buy.html,2020-02-12 12:23:59
  192.168.44.110,zhubajie,https://icbc.com.cn/pay.html,2020-02-12 12:24:03
  192.168.38.135,tangseng,https://icbc.com.cn/pay.html,2020-02-12 12:24:04
  192.168.89.189,sunwukong,https://icbc.com.cn/pay.html,2020-02-12 12:24:05
  192.168.44.110,zhubajie,https://icbc.com.cn/login.html,2020-02-12 12:24:04
  192.168.38.135,tangseng,https://icbc.com.cn/login.html,2020-02-12 12:24:08
  192.168.89.189,sunwukong,https://icbc.com.cn/login.html,2020-02-12 12:24:07
  192.168.38.135,tangseng,https://icbc.com.cn/pay.html,2020-02-12 12:24:10
  192.168.44.110,zhubajie,https://icbc.com.cn/pay.html,2020-02-12 12:24:06
  192.168.89.189,sunwukong,https://icbc.com.cn/pay.html,2020-02-12 12:24:09
  192.168.38.135,tangseng,https://icbc.com.cn/pay.html,2020-02-12 12:24:13
  192.168.44.110,zhubajie,https://icbc.com.cn/pay.html,2020-02-12 12:24:12
  192.168.89.189,sunwukong,https://icbc.com.cn/pay.html,2020-02-12 12:24:15
  ~~~

  * 整理之后的格式如下：

  ~~~
  192.168.145.77,sunwukong,https://icbc.com.cn/login.html,2020-02-12 12:23:47
  192.168.145.77,sunwukong,https://icbc.com.cn/transfer.html,2020-02-12 12:23:49
  192.168.145.77,sunwukong,https://icbc.com.cn/save.html,2020-02-12 12:23:52
  192.168.145.77,sunwukong,https://icbc.com.cn/buy.html,2020-02-12 12:23:58
  192.168.89.189,sunwukong,https://icbc.com.cn/pay.html,2020-02-12 12:24:05
  192.168.89.189,sunwukong,https://icbc.com.cn/login.html,2020-02-12 12:24:07
  192.168.89.189,sunwukong,https://icbc.com.cn/pay.html,2020-02-12 12:24:09
  192.168.89.189,sunwukong,https://icbc.com.cn/pay.html,2020-02-12 12:24:15

  192.168.52.100,zhubajie,https://icbc.com.cn/login.html,2020-02-12 12:23:45
  192.168.52.100,zhubajie,https://icbc.com.cn/transfer.html,2020-02-12 12:23:47
  192.168.52.100,zhubajie,https://icbc.com.cn/save.html,2020-02-12 12:23:53
  192.168.52.100,zhubajie,https://icbc.com.cn/buy.html,2020-02-12 12:23:59
  192.168.44.110,zhubajie,https://icbc.com.cn/pay.html,2020-02-12 12:24:03
  192.168.44.110,zhubajie,https://icbc.com.cn/login.html,2020-02-12 12:24:04
  192.168.44.110,zhubajie,https://icbc.com.cn/pay.html,2020-02-12 12:24:06
  192.168.44.110,zhubajie,https://icbc.com.cn/pay.html,2020-02-12 12:24:12

  192.168.54.172,tangseng,https://icbc.com.cn/login.html,2020-02-12 12:23:46
  192.168.54.172,tangseng,https://icbc.com.cn/transfer.html,2020-02-12 12:23:48
  192.168.54.172,tangseng,https://icbc.com.cn/save.html,2020-02-12 12:23:54
  192.168.54.172,tangseng,https://icbc.com.cn/buy.html,2020-02-12 12:23:57
  192.168.38.135,tangseng,https://icbc.com.cn/pay.html,2020-02-12 12:24:04
  192.168.38.135,tangseng,https://icbc.com.cn/login.html,2020-02-12 12:24:08
  192.168.38.135,tangseng,https://icbc.com.cn/pay.html,2020-02-12 12:24:10
  192.168.38.135,tangseng,https://icbc.com.cn/pay.html,2020-02-12 12:24:13
  ~~~
 */