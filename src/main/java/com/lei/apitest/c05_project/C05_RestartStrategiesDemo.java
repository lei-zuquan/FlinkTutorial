package com.lei.apitest.c05_project;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: 2020-06-10 14:43
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */

// Flink 的重启策略
public class C05_RestartStrategiesDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 只有开启了checkpointing 才会有重启策略，默认保存到JobManager中的内存中
        env.enableCheckpointing(5000); // 开启，检查点周期，单位毫秒；默认是-1，不开启

        // 默认的重启策略是固定延迟无限重启
        //env.getConfig().setRestartStrategy(RestartStrategies.fallBackRestart());
        // 设置固定延迟固定次数重启
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000));

        DataStreamSource<String> lines = env.socketTextStream("node-01", 7777);
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

        env.execute("C05_RestartStrategiesDemo");
    }
}
