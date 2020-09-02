package com.lei.apitest.c05_project;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
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

// 自定义State状态计算
// ValueState是保存分组后各自数据的状态信息，因此可以只保存其出现的数量即可
// 优化后的mapWithState

public class C08_MapWithStateV2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(5000);
        env.setStateBackend(new FsStateBackend("file:\\\\lei_test_project\\idea_workspace\\FlinkTutorial\\check_point_dir"));

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

            private transient ValueState<Integer> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                // 初始化状态或恢复历史状态
                // 定义一个状态描述器
                ValueStateDescriptor descriptor = new ValueStateDescriptor(
                        "wc-keyed-state",
                        Types.INT);
                valueState = getRuntimeContext().getState(descriptor);
            }

            @Override
            public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {

                Integer historyValue = valueState.value();
                if (historyValue != null) {
                    value.f1 += historyValue;
                }
                valueState.update(value.f1);
                return value;
            }
        });

        summed.print();

        env.execute("C08_MapWithStateV2");
    }
}
