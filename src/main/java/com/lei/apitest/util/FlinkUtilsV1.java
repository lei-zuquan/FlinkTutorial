package com.lei.apitest.util;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 10:02 上午 2020/6/8
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
public class FlinkUtilsV1 {

    private static final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    public static DataStream<String> createKafkaStream(String[] args, SimpleStringSchema simpleStringSchema) {
        String topic = args[0];
        String groupId = args[1];
        String brokerList = args[2];

        Properties props = new Properties();

        // 指定Kafka的Broker地址
        props.setProperty("bootstrap.servers", brokerList);
        // 提定组ID
        props.setProperty("group.id", groupId);
        // 如果没有记录偏移量，第一次从开始消费
        props.setProperty("auto.offset.reset", "earliest");
        // kafka的消费者不自动提交偏移量，默认kafka自动提交offset,且保存在__consumer_offsets
        // props.setProperty("enable.auto.commit", "false");

        // kafkaSource
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>(
                topic,
                new SimpleStringSchema(), // 序列化与反序列化方式
                props);

        // Source
        DataStream<String> lines = env.addSource(kafkaSource);
        return lines;
    }

    public static StreamExecutionEnvironment getEnv() {
        return env;
    }
}
