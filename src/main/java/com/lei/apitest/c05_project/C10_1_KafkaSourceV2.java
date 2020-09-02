package com.lei.apitest.c05_project;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @Author:
 * @Date: Created in 10:01 上午 2020/6/13
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
// 深入理解KafkaConsumer的偏移量存储位置

/*
 1.测试Kafka offset偏移量checkPoint保存
 2.Kafka 开启checkpoint策略，会将offset偏移量保存到StateBackEnd，同时默认也会将offset保存到Kafka特殊的topic（__consumer_offsets）
 3.开发人员，可以自已关闭向offset保存到Kafka特殊的topic (不推荐这样做，因为后续监控数据消费状态)
 4.如果应用出现故障后，没有指定恢复的checkPoint目录（优先从checkpoint目录），则Flink会从 Kafka特殊的topic读取偏移量，继续消费

/
 */

public class C10_1_KafkaSourceV2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 开启CheckPointing，同时开启重启策略
        env.enableCheckpointing(5000);
        // 设置StateBackend
        env.setStateBackend(new FsStateBackend("ile:\\\\lei_test_project\\idea_workspace\\FlinkTutorial\\check_point_dir"));
        // 取消任务checkPoint不删除
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 设置checkPoint的模式
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        Properties props = new Properties();

        // activity10 group_id_flink node-01:9092,node-02:9092,node-03:9092
        // 指定Kafka的Broker地址
        props.setProperty("bootstrap.servers", "node-01:9092,node-02:9092,node-03:9092");
        // 提定组ID
        props.setProperty("group.id", "group_id_flink");
        // 如果没有记录偏移量，第一次从开始消费
        props.setProperty("auto.offset.reset", "earliest");
        //props.setProperty("key,deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // kafka的消费者不自动提交偏移量，默认kafka自动提交offset,且保存在__consumer_offsets
        props.setProperty("enable.auto.commit", "false");

        // KafkaSource
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>(
                "topic",
                new SimpleStringSchema(),
                props);

        // Flink CheckPoint成功后还要向Kafka特殊的topic中写入偏移量
        // 生产环境，不建议关闭向kafka物殊的topic（__consumer_offsets）
        //kafkaSource.setCommitOffsetsOnCheckpoints(false);

        DataStreamSource<String> words = env.socketTextStream("localhost", 7777);
        words.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                if (value.startsWith("null")) {
                    System.out.println( 1 /0);
                }
                return value;
            }
        }).print();

        // Source
        DataStreamSource<String> lines = env.addSource(kafkaSource);

        // Sink
        lines.print();

        env.execute("C10_KafkaSourceV2");
    }
}
