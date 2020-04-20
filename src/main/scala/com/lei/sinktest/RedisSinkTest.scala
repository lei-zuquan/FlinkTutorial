package com.lei.sinktest

import com.lei.apitest.SensorReading
import com.lei.util.MyRedisUtil
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 10:45 下午 2020/4/20
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
class RedisSinkTest {

  def main(args: Array[String]): Unit = {


    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // source
    val inputStream: DataStream[String] = env.readTextFile("input_dir/sensor.txt")

    // Transform操作
    val dataStream: DataStream[SensorReading] = inputStream.map(data => {
      val dataArray: Array[String] = data.split(",")
      // 转成String 方便序列化输出
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })

    // sink
    dataStream.addSink(MyRedisUtil.getRedisSink())
    /*
        centos redis: redis-cli
        hset channal_sum xiaomi 100
        hset channal_sum huawei 100
        keys *
        hgetall channal_sum
     */
    // 把结果存入redis   hset  key:channel_sum   field:  channel   value:  count

    env.execute("redis sink test")
  }
}

