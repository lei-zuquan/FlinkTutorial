package com.flink_demo;

import com.lei.apitest.util.FlinkUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: 2020-06-10 14:43
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
/*


 */

public class T04_WordCountWithKafka {
    public static void main(String[] args) throws Exception {

        // 状态后端数据存储应该存储在分布式文件系统里，便于管理维护；
        // 特别说明，如果提交到集群上运行，不需要setProperty
        //System.setProperty("HADOOP_USER_NAME", "root");
        //System.setProperty("hadoop.home.dir", "/opt/cloudera/parcels/CDH-5.16.1-1.cdh5.16.1.p0.3/bin/");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 只有开启了checkpointing 才会有重启策略
        env.enableCheckpointing(5000); // 开启，检查点周期，单位毫秒；默认是-1，不开启

        // 默认的重启策略是固定延迟无限重启
        //env.getConfig().setRestartStrategy(RestartStrategies.fallBackRestart());
        // 设置固定延迟固定次数重启
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000));

        // 设置状态数据存储的后端，本地文件系统
        // 生产环境将StateBackend保存到分布式文件系统，且flink不建议在代码里写checkpoint目录代码，通过flink配置文件进行指定
        //env.setStateBackend(new FsStateBackend("hdfs://node-01:8020/user/root/flink_checkpoint"));
        //env.setStateBackend(new FsStateBackend(args[1]));

        // 程序异常退出或人为cancel掉，不删除checkpoint的数据
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);


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

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = lines.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String word) throws Exception {
                if (word.startsWith("null")) {
                    throw new RuntimeException("输入为null，发生异常");
                }
                return Tuple2.of(word, 1);
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> summed = wordAndOne.keyBy(0).sum(1);

        summed.print();

        FlinkUtils.getEnv().execute("T04_WordCountPro");
    }
}
