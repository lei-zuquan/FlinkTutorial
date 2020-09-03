package com.lei.apitest.c05_project;

import com.lei.apitest.c05_project.domain.ActivityBean;
import com.lei.apitest.util.FlinkUtilsV1;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import java.util.concurrent.TimeUnit;

/**
 * @Author:
 * @Date: 2020-06-09 15:11
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */

/*
3.向Kafka-producer生产数据

u001,A1,2019-09-02 10:10:11,1,115.908923,39.267291
u001,A2,2019-09-02 10:10:11,1,115.908923,39.267291
u001,A3,2019-09-02 10:10:11,1,123.818817,41.312458
u001,A4,2019-09-02 10:10:11,1,121.26757,37.49794
u002,A1,2019-09-02 10:11:11,1,121.26757,37.49794
u001,A1,2019-09-02 10:11:11,2,116.311805,40.028572

 */

// 1、统计基于活动id/event_type 数量总和
// 2、统计基于活动id/event_type/province 数量总和
// 并将结果写入MySQL/Redis 数据存储中

public class C04_ActivityCount {
    public static void main(String[] args) throws Exception {
        // 输入参数：activity10 group_id_flink node-01:9092,node-02:9092,node-03:9092
        DataStream<String> lines = FlinkUtilsV1.createKafkaStream(args, new SimpleStringSchema());

        //SingleOutputStreamOperator<ActivityBean> beans = lines.map(new C01_DataToActivityBeanFunction());
        SingleOutputStreamOperator<ActivityBean> beans = AsyncDataStream.unorderedWait(
                // 这里的队列不能超过最大队列大小
                lines, new C02_AsyncGeoToActivityBeanFunction(), 0, TimeUnit.MILLISECONDS, 10);

        SingleOutputStreamOperator<ActivityBean> summed1 = beans.keyBy("aid", "eventType").sum("count");

        SingleOutputStreamOperator<ActivityBean> summed2 = beans.keyBy("aid", "eventType","province").sum("count");

        // 调用Sink
        summed1.addSink(new C04_MysqlSink());

        // 创建一个Redis conf
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder()
                .setHost("redis-01")
                .setPort(6379)
                .setDatabase(3)
                .build();

        summed2.addSink(new RedisSink<ActivityBean>(conf, new RedisExampleMapper()));

        FlinkUtilsV1.getEnv().execute("C04_ActivityCount");
    }

    public static class RedisExampleMapper implements RedisMapper<ActivityBean> {

        // 调用Redis的写入方法
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "ACT_COUNT");
        }

        // 写入Redis中key
        @Override
        public String getKeyFromData(ActivityBean data) {
            return data.aid + "_" + data.eventType + "_" + data.province;
        }

        // Redis中的value
        @Override
        public String getValueFromData(ActivityBean data) {
            return String.valueOf(data.count);
        }
    }
}
