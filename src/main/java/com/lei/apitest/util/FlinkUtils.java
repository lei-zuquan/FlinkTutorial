package com.lei.apitest.util;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 9:52 上午 2020/6/13
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
// Flink高级工具类封装

public class FlinkUtils {

    // 创建Flink 流式计算执行环境
    private static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // 为了通用性，将工具类尽量抽象化，使用泛型可达到此目的
    public static <T> DataStream<T> createKafkaStream(
            ParameterTool parameters,
            Class<? extends DeserializationSchema<T>> clazz) throws IllegalAccessException, InstantiationException {

        // 设置全局的参数，以后在自种算子里就可以拿到全局参数
        env.getConfig().setGlobalJobParameters(parameters);

        // 开启CheckPointing，同时开启重启策略
        env.enableCheckpointing(parameters.getLong("checkpoint-interval", 5000L), CheckpointingMode.EXACTLY_ONCE);
        // 设置固定延迟固定次数重启，默认无限重启
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000));
        // 设置StateBackend，生产环境不建议写在这里，建议写在flink-conf.yaml配置文件中；默认保存到内存里面
        // env.setStateBackend(new FsStateBackend("ile:\\\\lei_test_project\\idea_workspace\\FlinkTutorial\\check_point_dir"));
        // 取消任务checkPoint不删除
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 设置checkPoint的模式, 上述指定此处不需要再次指定
        // env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        Properties props = new Properties();

        // activity10 group_id_flink node-01:9092,node-02:9092,node-03:9092
        // 指定Kafka的Broker地址
        props.setProperty("bootstrap.servers", parameters.getRequired("bootstrap.servers"));
        // 提定组ID
        props.setProperty("group.id", parameters.getRequired("group.id"));
        // 如果没有记录偏移量，第一次从开始消费
        props.setProperty("auto.offset.reset", parameters.get("auto.offset.reset","earliest"));
        //props.setProperty("key,deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // kafka的消费者不自动提交偏移量，默认kafka自动提交offset,且保存在__consumer_offsets
        props.setProperty("enable.auto.commit", parameters.get("enable.auto.commit","false"));

        // 获取topics
        String topics = parameters.getRequired("topics");
        String[] split = topics.split(",");
        List<String> topicList = Arrays.asList(split);

        // KafkaSource
        FlinkKafkaConsumer011<T> kafkaConsumer = new FlinkKafkaConsumer011<T>(
                topicList,
                clazz.newInstance(),
                props);

        // Flink CheckPoint成功后还要向Kafka特殊的topic中写入偏移量, 默认是true
        // kafkaConsumer.setCommitOffsetsOnCheckpoints(true);

        return env.addSource(kafkaConsumer);
    }

    public static <T> DataStream<T> createKafkaStream(
            ParameterTool parameters, String topics, String groupId, Class<? extends DeserializationSchema<T>> clazz) throws IllegalAccessException, InstantiationException {
        // 设置全局的参数，以后在自种算子里就可以拿到全局参数
        env.getConfig().setGlobalJobParameters(parameters);

        // 开启CheckPointing，同时开启重启策略
        env.enableCheckpointing(parameters.getLong("checkpoint-interval", 5000L), CheckpointingMode.EXACTLY_ONCE);
        // 设置固定延迟固定次数重启，默认无限重启
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000));
        // 设置StateBackend，生产环境不建议写在这里，建议写在flink-conf.yaml配置文件中；默认保存到内存里面
        // env.setStateBackend(new FsStateBackend("ile:\\\\lei_test_project\\idea_workspace\\FlinkTutorial\\check_point_dir"));
        // 取消任务checkPoint不删除
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 设置checkPoint的模式, 上述指定此处不需要再次指定
        // env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        Properties props = new Properties();

        // activity10 group_id_flink node-01:9092,node-02:9092,node-03:9092
        // 指定Kafka的Broker地址
        props.setProperty("bootstrap.servers", parameters.getRequired("bootstrap.servers"));
        // 提定组ID
        props.setProperty("group.id", groupId);
        // 如果没有记录偏移量，第一次从开始消费
        props.setProperty("auto.offset.reset", parameters.get("auto.offset.reset","earliest"));
        //props.setProperty("key,deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // kafka的消费者不自动提交偏移量，默认kafka自动提交offset,且保存在__consumer_offsets
        props.setProperty("enable.auto.commit", parameters.get("enable.auto.commit","false"));

        // 获取topics
        String[] split = topics.split(",");
        List<String> topicList = Arrays.asList(split);

        // KafkaSource
        FlinkKafkaConsumer011<T> kafkaConsumer = new FlinkKafkaConsumer011<T>(
                topicList,
                clazz.newInstance(),
                props);

        // Flink CheckPoint成功后还要向Kafka特殊的topic中写入偏移量, 默认是true
        // kafkaConsumer.setCommitOffsetsOnCheckpoints(true);

        return env.addSource(kafkaConsumer);
    }


    public static StreamExecutionEnvironment getEnv() {
        return env;
    }
}
