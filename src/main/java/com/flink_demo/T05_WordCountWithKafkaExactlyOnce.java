package com.flink_demo;

import com.lei.apitest.util.FlinkUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

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

public class T05_WordCountWithKafkaExactlyOnce {
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
