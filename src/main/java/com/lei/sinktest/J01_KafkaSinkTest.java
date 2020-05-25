package com.lei.sinktest;

import com.lei.apitest.SensorReading;
import com.lei.util.J_MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: 2020-05-22 17:41
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
// 4、从Kafka中读取数据
/*
# 查看kafka主题列表
    kafka-topics --list --zookeeper node-01:2181,node-02:2181,node-03:2181
# 创建topic
    kafka-topics --create --zookeeper node-01:2181,node-02:2181,node-03:2181 --replication-factor 3 --partitions 3 --topic flink_topic_test
# 查看topic
    kafka-topics --zookeeper node-01:2181,node-02:2181,node-03:2181 --topic flink_topic_test --describe
# 创建生产者
    kafka-console-producer --broker-list node-01:9092,node-02:9092,node-03:9092 --topic flink_topic_test
# 创建消费者
    kafka-console-consumer --bootstrap-server node-01:9092,node-02:9092,node-03:9092 --topic flink_topic_test --group flink_topic_test_g1 --from-beginning
# 检查消费者消费数据情况
    kafka-consumer-groups --bootstrap-server node-01:9092,node-02:9092,node-03:9092 --describe --group flink_topic_test_g1
# 列出所有消费者的情况
    kafka-consumer-groups --bootstrap-server node-01:9092,node-02:9092,node-03:9092 --list
# 修改topic分区
    kafka-topics --zookeeper node-01:2181,node-02:2181,node-03:2181 --alter --topic flink_topic_test --partitions 4
# 删除消费者组
    kafka-consumer-groups --bootstrap-server node-01:9092,node-02:9092,node-03:9092 --delete --group flink_topic_test_g1
# 删除topic
    kafka-topics --delete --zookeeper node-01:2181,node-02:2181,node-03:2181 --topic flink_topic_test

 */
public class J01_KafkaSinkTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // source
        //val inputStream: DataStream[String] = env.readTextFile("input_dir/sensor.txt")
        DataStream<String> inputStream = env.addSource(J_MyKafkaUtil.getConsumer("flink_topic_test"));

        // Transform操作
        /*
         数据源示例：sensor_1, 1547718205, 27.1
         数据源示例：sensor_2, 1547718206, 28.1
         数据源示例：sensor_3, 1547718207, 29.1
         */
        DataStream<String> dataStream = inputStream.map(data -> {
            String[] dataArray = data.split(",");
            // 转成String 方便序列化输出
            return new SensorReading(dataArray[0].trim(), Long.valueOf(dataArray[1].trim()), Double.valueOf(dataArray[2].trim())).toString();
        });

        // sink
        // dataStream.addSink(new FlinkKafkaProducer011[String]("node-01:9092", "gmall", new SimpleStringSchema()))
        dataStream.addSink(J_MyKafkaUtil.getProducer("flink_topic_test_sink"));
        /*
        # 创建topic
            kafka-topics --create --zookeeper node-01:2181,node-02:2181,node-03:2181 --replication-factor 3 --partitions 3 --topic flink_topic_test
        # 查看topic
            kafka-topics --zookeeper node-01:2181,node-02:2181,node-03:2181 --topic flink_topic_test --describe
        # 创建生产者
            kafka-console-producer --broker-list node-01:9092,node-02:9092,node-03:9092 --topic flink_topic_test
        # 创建消费者
            kafka-console-consumer --bootstrap-server node-01:9092,node-02:9092,node-03:9092 --topic flink_topic_test --group flink_topic_test_g1 --from-beginning
         */
        dataStream.print();

        env.execute("kafka sink test");
    }
}

//class SensorReading {
//    String id;
//    Long timestamp;
//    Double temperature;
//
//    public SensorReading(String id, Long timestamp, Double temperature) {
//        this.id = id;
//        this.timestamp = timestamp;
//        this.temperature = temperature;
//    }
//
//    @Override
//    public String toString() {
//        return "J_SensorReading{" +
//                "id='" + id + '\'' +
//                ", timestamp=" + timestamp +
//                ", temperature=" + temperature +
//                '}';
//    }
//}
