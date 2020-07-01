package com.lei.apitest.c05_project;

import com.lei.apitest.util.FlinkUtils;
import com.lei.apitest.util.FlinkUtilsV1;
import com.lei.apitest.util.MyRedisSink;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.Collector;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 9:39 上午 2020/6/13
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */

/*
Flink高级工具类封装

--group.id g_10 --topics s1,s2

测试消费Kafka中的数据实现ExactlyOnce
【提交至集群测试】

同时，记得将配置文件上传到JobManager所在节点
web ui启动Flink应用，指定参数

将redis停掉
redis-cli
AUTH 123456

SHUTDOWN save  // 停掉且将数据保存，测试程序是否能正常消费；
               // 因为redis故障，TaskManager重启，重新读取数据、重新连接redis，连接不上，又重新启动

继续向kafka输入数据:spark spark hadoop hadoop

然后重启redis

查看redis数据结果是否正确

==========
2.将flink 任务cancel掉，再重新启动

    a.将flink 任务cancel掉
    b.继续向kafka输入数据，spark spark hadoop hadoop
    c.重启任务，并指定checkPoint目录（恢复）
    d.查看结果是否正常

barry机制，栅栏（隔离的意思），所有算子都成功才会checkpoint。保证任务全启完成后，才会更新偏移量
比如：
Source:Custom Source -> Flat Map -> Map  ------------------- Keyed Aggregation -> Map -> Sink
只有所有算子执行成功才会checkPoint，如果sink不成功或者中间算子执行失败都不会更新偏离量

barry机制会给数据打上id，数据被切分成很多很多数据，每个数据都会打上id，如果所有数据都成功了，才会更新数据的偏移量

上述是redis故障；下面对Job进行cancel掉，继续输入数据，再次启动Flink Job（指定恢复checkpoint目录）

 */
public class C10_2_FlinkKafkaToRedis {
    public static void main(String[] args) throws Exception {
        /*ParameterTool parameters = ParameterTool.fromArgs(args);
        */

        // 上述方式可以将传入的配置参数读取，但是在实际生产环境最佳放在指定的文件中
        // 而不是放在maven 项目的resources中，因为打完包后，想要修改就不太方便了
        // Flink 官方最佳实践
        ParameterTool parameters = ParameterTool.fromPropertiesFile(args[0]);
        // # topic:activity10 分区3，副本2
        // # 创建topic
        // kafka-topics --create --zookeeper node-01:2181,node-02:2181,node-03:2181 --replication-factor 2 --partitions 3 --topic activity10
        //
        // # 创建生产者
        // kafka-console-producer --broker-list node-01:9092,node-02:9092,node-03:9092 --topic activity10

        DataStream<String> lines = FlinkUtils.createKafkaStream(parameters, SimpleStringSchema.class);

        SingleOutputStreamOperator<Tuple2<String, Integer>> summed = lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(new Tuple2<String, Integer>(word, 1));
                }
            }
        }).keyBy(0).sum(1);

        // 数据格式转换及写入redis
        summed.map(new MapFunction<Tuple2<String, Integer>, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(Tuple2<String, Integer> tp) throws Exception {
                return Tuple3.of("Word_Count", tp.f0, tp.f1.toString());
            }
        }).addSink(new MyRedisSink());


        FlinkUtils.getEnv().execute("C10_FlinkKafkaToRedis");

    }
}
