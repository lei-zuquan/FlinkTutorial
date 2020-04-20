package com.lei.util

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 7:53 上午 2020/4/20
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */

// flink通过有状态支持，将kafka消费的offset自动进行状态保存，自动维护偏移量
object MyKafkaUtil {

  val prop = new Properties()

  val zk_servers = "node-01:9092,node-02:9092,node-03:9092"
  prop.setProperty("bootstrap.servers", zk_servers)
  prop.setProperty("group.id", "gmall")
  prop.setProperty("key,deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  prop.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  prop.setProperty("auto.offset.reset", "latest")

  def getConsumer(topic:String ):FlinkKafkaConsumer011[String]= {
    val myKafkaConsumer:FlinkKafkaConsumer011[String] = new FlinkKafkaConsumer011[String](topic, new SimpleStringSchema(), prop)
    myKafkaConsumer
  }

  def getProducer(topic: String): FlinkKafkaProducer011[String] = {
    new FlinkKafkaProducer011[String]("node-01:9092", "gmall", new SimpleStringSchema())
  }


}
