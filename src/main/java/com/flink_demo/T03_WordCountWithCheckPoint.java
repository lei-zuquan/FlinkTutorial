package com.flink_demo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 5:04 上午 2020/7/6
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */

/*
 Flink 使用状态后端保存异常时业务状态信息，处理异常版WordCount
 */
public class T03_WordCountWithCheckPoint {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 只有开启了checkpointing 才会有重启策略，默认保存到JobManager中的内存中
        env.enableCheckpointing(5000); // 开启，检查点周期，单位毫秒；默认是-1，不开启

        // 默认的重启策略是固定延迟无限重启
        //env.getConfig().setRestartStrategy(RestartStrategies.fallBackRestart());
        // 设置固定延迟固定次数重启
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000));

        // source
        DataStreamSource<String> lines = env.socketTextStream("localhost", 7777);

        // transformation
        SingleOutputStreamOperator<Tuple2<String, Integer>> summed = lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                if (line.startsWith("null") ) {
                    throw new RuntimeException("输入为null，发生异常");
                }

                String[] words = line.split(" ");
                for (String word : words) {
                    collector.collect(Tuple2.of(word, 1));
                }
            }
        }).keyBy(0).sum(1);

        // sink
        summed.print();

        env.execute("T03_WordCountWithCheckPoint");
    }
}
