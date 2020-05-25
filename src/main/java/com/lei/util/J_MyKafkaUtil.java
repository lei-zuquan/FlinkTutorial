package com.lei.util;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

import java.util.Properties;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: 2020-05-20 17:06
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */

// flink通过有状态支持，将kafka消费的offset自动进行状态保存，自动维护偏移量
public class J_MyKafkaUtil {

    private static Properties prop = new Properties();
    private static String zk_servers = "node-01:9092,node-02:9092,node-03:9092";

    static {
        prop.setProperty("bootstrap.servers", zk_servers);
        prop.setProperty("group.id", "flink_topic_test_g1");
        prop.setProperty("key,deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.setProperty("auto.offset.reset", "latest");
    }


    public static FlinkKafkaConsumer011<String> getConsumer(String topic) {
        FlinkKafkaConsumer011<String> myKafkaConsumer = new FlinkKafkaConsumer011<> (topic, new SimpleStringSchema(), prop);
        return myKafkaConsumer;
    }

    public static FlinkKafkaProducer011<String> getProducer(String topic) {
        return new FlinkKafkaProducer011<> (zk_servers, topic, new SimpleStringSchema());
    }
}
