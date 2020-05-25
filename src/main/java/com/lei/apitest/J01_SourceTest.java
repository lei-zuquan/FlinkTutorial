package com.lei.apitest;

import com.lei.domain.J_SensorReading;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: 2020-05-20 16:46
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
public class J01_SourceTest {
    public static void main(String[] args) throws Exception {
        //ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 1、自定义集合读取数据
        /*DataStreamSource<J_SensorReading> stream1 = env.fromCollection(Arrays.asList(
                new J_SensorReading("sensor_1", 1547718199L, 35.80018327300259),
                new J_SensorReading("sensor_6", 1547718201L, 18.402894393403084),
                new J_SensorReading("sensor_7", 1547718202L, 6.720945201171228),
                new J_SensorReading("sensor_10", 1547718205L, 38.101067604893444)
        ));
        stream1.setParallelism(1).print();*/

        // 2、从文件中读取数据
        /*DataStreamSource<String> stream2 = env.readTextFile("input_dir/sensor.txt");
        stream2.print().setParallelism(1);*/

        // 3、从Element中读取数据
        env.fromElements("1.0", "2.0", "String").print();

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
            kafka-consumer-groups --bootstrap-server node-01:9092,node-02:9092,node-03:9092 --delete --group  flink_topic_test_g1
        # 删除topic
            kafka-topics --delete --zookeeper node-01:2181,node-02:2181,node-03:2181 --topic flink_topic_test

         */
        //StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        /*DataStreamSource<String> kafkaStream = env.addSource(MyKafkaUtil.getConsumer("flink_topic_test"));
        kafkaStream.print().setParallelism(1);*/

        // 5、自定义source
        /*DataStream<J_SensorReading> stream4 = env.addSource(new J_SensorSource());
        stream4.print("stream4").setParallelism(1);*/

        env.execute("SourceTest");
    }
}


class J_SensorSource implements SourceFunction<J_SensorReading> {

    // 定义一个flag，表示数据源是否正常运行
    Boolean running = true;

    @Override
    public void run(SourceContext<J_SensorReading> sourceContext) throws Exception {
        // 初始化一个随机数发生器
        Random random = new Random();

        // 隔一段时间，传感器温度发生变化
        // 初始化定义一组传感器温度数据
        List<Tuple2<String, Double>> list = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            // 下一个高斯分布值，正态分布
            list.add(new Tuple2<>("sensor_" + i, 60 + random.nextGaussian() * 20));
        }

        List<Tuple2<String, Double>> tempList = new ArrayList<>();
        // 用无限循环，产生数据流
        while (running){
            // 在前一次温度的基础上更新温度值
            tempList.clear();
            for (Tuple2<String, Double> t : list) {
                tempList.add(new Tuple2<String, Double>(t.f0, t.f1 + random.nextGaussian()));
            }
            // 获取当前时间戳
            Long curTime = System.currentTimeMillis();
            tempList.forEach( t -> sourceContext.collect(new J_SensorReading(t.f0, curTime, t.f1)));

            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}