package com.lei.apitest.c02_transformation;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 5:51 下午 2020/6/6
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */

/*
 keyBy是shuffle算子
 在Flink中叫redistrute
 */
public class C05_KeyByDemo2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 直接输入的就是单词
        DataStreamSource<String> words = env.socketTextStream("localhost", 7777);

        SingleOutputStreamOperator<C05_WordCounts> wordAndOne = words.flatMap(new FlatMapFunction<String, C05_WordCounts>() {
            @Override
            public void flatMap(String value, Collector<C05_WordCounts> collector) throws Exception {
                collector.collect(new C05_WordCounts(value, 1L));
            }
        });

        // 在java，认为元素是一个特殊的集合，脚标是从0开始；因为Flink底层源码是java编写的
        //KeyedStream<C05_WordCounts, String> keyed = wordAndOne.keyBy(t -> t.getWord());
        KeyedStream<C05_WordCounts, Tuple> keyed = wordAndOne.keyBy("word");

        // 聚合
        SingleOutputStreamOperator<C05_WordCounts> sumed = keyed.sum("counts");

        sumed.print();

        env.execute("C04_KeyByDemo1");
    }
}
