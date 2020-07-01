package com.lei.apitest.c05_project;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: 2020-06-12 8:55
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
public class C09_4_OperatorStateDemoV2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        // 开启checkpoint，并设置checkpoint间隔；默认不开启checkPoint
        env.enableCheckpointing(5000);
        // 设置故障重启次数，重启2次，重启间隔2秒；默认无限重启
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, 2000));
        // 设置checkpoint策略，为本地文件存储；默认内存存储; 生产环境建议使用hdfs分布式文件存储且配置在flink-conf.yaml文件中
        env.setStateBackend(new FsStateBackend("file:///Users/leizuquan/IdeaProjects/FlinkTutorial/check_point_dir"));
        // 设置Job被cancel掉后或故障下线后，checkpoint不删除；默认checkpoint在Job下线后会删除
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);;

        DataStreamSource<String> lines = env.socketTextStream("localhost", 7777);
        lines.map(new MapFunction<String, String>() {
            @Override
            public String map(String line) throws Exception {
                if (line.equals("null")) {
                    System.out.println(1 / 0);
                }
                return line;
            }
        }).print();

        DataStreamSource<Tuple2<String, String>> tp = env.addSource(new C09_3_MyExactlyOnceParFileSource("MyParFile"));

        tp.print();


        env.execute("C09_OperatorStateDemo");
    }
}
