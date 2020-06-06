package com.lei.apitest.c02_transformation;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 4:56 下午 2020/6/6
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */

/*
 对DataStream进行操作，返回一个新的DataStream

 */
public class C02_FlatMap_TransformationDemo1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> lines = env.fromElements("spark flink hadoop", "spark flink hbase");

        SingleOutputStreamOperator<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> collector) throws Exception {
                /*String[] words = line.split(" ");
                for (String word : words) {
                    collector.collect(word);
                }*/

                //Arrays.asList(line.split(" ")).forEach(w -> collector.collect(w));
                Arrays.stream(line.split(" ")).forEach(collector::collect); // 推荐使用这种方式编写代码，简洁
            }
        });

        SingleOutputStreamOperator<String> words2 = lines.flatMap((String line, Collector<String> out) ->
                Arrays.stream(line.split(" ")).forEach(out::collect)).returns(Types.STRING);

        // flatMap方法还可以传入RichFlatMapFunction

        // Sink
        words2.print();

        env.execute("C02_FlatMap_TransformationDemo1");
    }
}
