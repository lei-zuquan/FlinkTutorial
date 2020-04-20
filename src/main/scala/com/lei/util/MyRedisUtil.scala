package com.lei.util

import com.lei.apitest.SensorReading
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 11:09 上午 2020/4/18
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
object MyRedisUtil {

  val conf = new FlinkJedisPoolConfig.Builder().setHost("hadoop1").setPort(6379).build()

  def getRedisSink(): RedisSink[SensorReading] = {
    new RedisSink(conf, new MyRedisMapper)
  }
}

class MyRedisMapper extends RedisMapper[SensorReading]{
  // 定义保存数据到redis的命令
  override def getCommandDescription: RedisCommandDescription = {
    // 把传感器id和温度值保存成哈希表 HSET key field value
    new RedisCommandDescription(RedisCommand.HSET, "sensor_temperature")
    // new RedisCommandDescription(RedisCommand.SET  )
  }

  // 定义保存到redis的value
  override def getValueFromData(t: SensorReading): String = t.temperature.toString

  // 定义保到到redis的key
  override def getKeyFromData(t: SensorReading): String = t.id
}
