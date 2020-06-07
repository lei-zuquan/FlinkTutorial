package com.lei.apitest.c03_sink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 12:07 下午 2020/6/7
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */

/*
 Flink 的资源槽，默认的名字都是default

 */
public class C06_SharingGroupTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> lines = env.socketTextStream("localhost", 7777);

        SingleOutputStreamOperator<String> word = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> out) throws Exception {
                String[] words = line.split(" ");
                for (String word : words) {
                    out.collect(word);
                }
            }
        }).slotSharingGroup("lei_group");

        SingleOutputStreamOperator<String> filtered = word.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return value.startsWith("h");
            }
        });
        //.disableChaining(); // 将这个算子单独划分处理，生成一个Task，跟其他的算子不再有Operator Chain; 比如：CPU、内存密集型，算法逻辑复杂的操作单独划分成Task，独享硬件资源
        //.startNewChain();
        // 从该算子开始，开启一个新的链；从这个算子之前，发生redistributing
        // 需要使用Flink web 查看Show Plan

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = filtered.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value, 1);
            }
        });


        SingleOutputStreamOperator<Tuple2<String, Integer>> summed = wordAndOne.keyBy(0).sum(1);

        summed.print();

        env.execute("C05_Task_SubTask_ChainTest");
    }

















}
