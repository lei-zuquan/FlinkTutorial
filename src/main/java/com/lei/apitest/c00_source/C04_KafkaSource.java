package com.lei.apitest.c00_source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 11:46 上午 2020/6/6
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */

/*
  从kafka中读取数据的Source,可以并行的Source，并且可以实现ExactlyOnce
 */
public class C04_KafkaSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();

        // activity10 group_id_flink node-01:9092,node-02:9092,node-03:9092
        // 指定Kafka的Broker地址
        props.setProperty("bootstrap.servers", "node-01:9092,node-02:9092,node-03:9092");
        // 提定组ID
        props.setProperty("group.id", "group_id_flink");
        // 如果没有记录偏移量，第一次从开始消费
        props.setProperty("auto.offset.reset", "earliest");
        props.setProperty("key,deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // kafka的消费者不自动提交偏移量，默认kafka自动提交offset,且保存在__consumer_offsets
        // props.setProperty("enable.auto.commit", "false");

        // kafkaSource
        FlinkKafkaConsumer011<String> kafkaSource = new FlinkKafkaConsumer011<>(
                "activity10",
                new SimpleStringSchema(), // 序列化与反序列化方式
                props);

        // Source
        DataStream<String> lines = env.addSource(kafkaSource);


        // Sink
        lines.print();

        env.execute("C03_KafkaSource");
    }
}
