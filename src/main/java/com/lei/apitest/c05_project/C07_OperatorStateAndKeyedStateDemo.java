package com.lei.apitest.c05_project;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

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
  目的：用来观察OperatorState和KeyedState
  Kafka消费者消费数据记录偏移量，消费者对应SubTask使用OperatorState记录偏移量
  KeyBy之后，进行聚合操作，进行历史数据集累加，这些SubTask使用累加分组后的历史就是KeyedState

  创建topic，有4个分区的topic，这样Job，就是因为keyBy而生成2个Task

  运行程序，前检查一下flink-conf.yaml文件是否配置了checkpoint
  运行程序后，查看hdfs，是否有8个文件，其中有4个是keyedState，保存kafka的offset，有4个文件是operatorState，保存中间结果
 */
public class C07_OperatorStateAndKeyedStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 开启checkpoint策略,定时5秒checkpoint一次
        env.enableCheckpointing(5000);
        // 为了实现EXACTLY_ONCE, 必须记录偏移量
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 设置固定延迟固定次数重启，默认是无限重启
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000));
        // 设置Job被cancel或异常后，checkpoint不删除
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        Properties props = new Properties();
        // activity10 group_id_flink node-01:9092,node-02:9092,node-03:9092
        // 指定Kafka的Broker地址
        props.setProperty("bootstrap.servers", "node-01:9092,node-02:9092,node-03:9092");
        // 提定组ID
        props.setProperty("group.id", args[0]);
        // 如果没有记录偏移量，第一次从开始消费
        props.setProperty("auto.offset.reset", "earliest");
        props.setProperty("key,deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // kafka的消费者不自动提交偏移量，默认kafka自动提交offset,且保存在__consumer_offsets
        props.setProperty("enable.auto.commit", "false");

        // kafkaSource
        FlinkKafkaConsumer011<String> kafkaSource = new FlinkKafkaConsumer011<>(
                args[1],
                new SimpleStringSchema(), // 序列化与反序列化方式
                props);

        // Source
        DataStream<String> lines = env.addSource(kafkaSource);

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = line.split(" ");
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        });

        // 为了保证程序出现问题还可以继续累加，要记录分组聚合的中间结果
        SingleOutputStreamOperator<Tuple2<String, Integer>> summed = wordAndOne.keyBy(0).sum(1);

        // Sink
        summed.print();

        env.execute("C07_OperatorStateAndKeyedStateDemo");
    }
}
