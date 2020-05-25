package com.lei.util;

import com.lei.domain.J_SensorReading;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: 2020-05-25 14:54
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
public class J_MyRedisUtil {

    private static FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("localhost").setPort(6379).build();

    public static RedisSink<J_SensorReading> getRedisSink() {
        return new RedisSink(conf, new J_MyRedisMapper());
    }
}

class J_MyRedisMapper implements RedisMapper<J_SensorReading> {

    // 定义保存数据到redis的命令
    @Override
    public RedisCommandDescription getCommandDescription() {
        // 把传感器id和温度值保存成哈希表 HSET key field value
        return new RedisCommandDescription(RedisCommand.HSET, "sensor_temperature");
        // new RedisCommandDescription(RedisCommand.SET  )
    }

    @Override
    public String getKeyFromData(J_SensorReading sensorReading) {
        return sensorReading.id;
    }

    @Override
    public String getValueFromData(J_SensorReading sensorReading) {
        return sensorReading.temperature + "";
    }

}