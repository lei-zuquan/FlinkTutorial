package com.lei.apitest.c05_project;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: 2020-06-10 14:43
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
// Flink 的StateBackend的使用
public class C06_StateBackendDemo {
    public static void main(String[] args) throws Exception {

        // 状态后端数据存储应该存储在分布式文件系统里，便于管理维护
        System.setProperty("HADOOP_USER_NAME", "root");
        System.setProperty("hadoop.home.dir", "/opt/cloudera/parcels/CDH-5.16.1-1.cdh5.16.1.p0.3/bin/");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 只有开启了checkpointing，重启策略才会生效；默认不开启重启策略
        env.enableCheckpointing(5000); // 开启，检查点周期，单位毫秒；默认是-1，不开启

        // 默认的重启策略是固定延迟无限重启
        //env.getConfig().setRestartStrategy(RestartStrategies.fallBackRestart());
        // 设置固定延迟固定次数重启；默认是无限重启
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000));

        // 设置状态数据存储的后端，本地文件系统；默认：状态保存在 TaskManager 的内存中，检查点保存在 JobManager 的内存中
        env.setStateBackend(new FsStateBackend("file:\\\\lei_test_project\\idea_workspace\\FlinkTutorial\\check_point_dir"));
        // 生产环境将StateBackend保存到分布式文件系统
        //env.setStateBackend(new FsStateBackend("hdfs://node-01:8020/user/root/sqoop/flink_state_backend"));

        // 程序异常退出或人为cancel掉，不删除checkpoint的数据；默认是会删除Checkpoint数据
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

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

        env.execute("C06_StateBackendDemo");
    }
}
