package com.lei.apitest.c05_project;

import com.lei.apitest.util.FlinkUtils;
import com.lei.apitest.util.FlinkUtilsV1;
import com.lei.apitest.util.MyRedisSink;
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

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 9:39 上午 2020/6/13
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */

/*

--group.id g_10 --topics s1,s2

 */
public class C10_FlinkKafkaToRedis {
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

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = lines.map(w -> Tuple2.of(w, 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT));

        // 在java，认为元素是一个特殊的集合，脚标是从0开始；因为Flink底层源码是java编写的
        KeyedStream<Tuple2<String, Integer>, Tuple> keyed = wordAndOne.keyBy(0);
        SingleOutputStreamOperator<Tuple2<String, Integer>> summed = keyed.sum(1);

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
