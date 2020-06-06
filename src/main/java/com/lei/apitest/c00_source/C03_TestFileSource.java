package com.lei.apitest.c00_source;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 11:31 上午 2020/6/6
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
/*
    从文件夹加载数据，运行完后程序就会停止
    数据源多并行度
 */
public class C03_TestFileSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> lines = env.readTextFile("input_dir");

        int parallelism = lines.getParallelism();
        System.out.println("+++++>" + parallelism);

        SingleOutputStreamOperator<Tuple2<String, Integer>> words = lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = line.split(" ");
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        });

        int parallelism1 = words.getParallelism();
        System.out.println("+++++>" + parallelism1);

        SingleOutputStreamOperator<Tuple2<String, Integer>> summed = words.keyBy(0).sum(1);
        summed.print();

        env.execute("C03_TestFileSource");
    }
}
