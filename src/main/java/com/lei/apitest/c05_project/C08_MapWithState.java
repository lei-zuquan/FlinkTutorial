package com.lei.apitest.c05_project;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author:
 * @Date: 2020-06-11 9:46
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */

// 自定义State状态计算：实现单词出现次数累加求和功能
public class C08_MapWithState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> lines = env.socketTextStream("node-01", 7777);
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = line.split(" ");
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        });

        KeyedStream<Tuple2<String, Integer>, Tuple> keyed = wordAndOne.keyBy(0);

        SingleOutputStreamOperator<Tuple2<String, Integer>> summed = keyed.map(new RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {

            private transient ValueState<Tuple2<String, Integer>> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                // 初始化状态或恢复历史状态
                // 定义一个状态描述器
                ValueStateDescriptor descriptor = new ValueStateDescriptor(
                        "wc-keyed-state",
                        TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
                        }));
                valueState = getRuntimeContext().getState(descriptor);
            }

            @Override
            public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {

                Tuple2<String, Integer> historyValue = valueState.value();
                if (historyValue != null) {
                    value.f1 += historyValue.f1;
                }
                valueState.update(value);
                return value;
            }
        });

        summed.print();

        env.execute("C08_MapWithState");
    }
}
