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
/*
 从指定的SavePoint路径恢复历史状态数据

flink standalone向hdfs中checkpoint数据
注意：
    1.需要向flink集群中的节点拷贝hadoop依赖
        cp flink-shaded-hadoop-2-uber-2.7.5-7.0.jar /bigdata/flink-1.9.1/lib
    2.将hadoop依赖分发到其他节点
        for i in {2..3}; do scp flink-shaded-hadoop-2-uber-2.7.5-7.0.jar node-$i.51doit.cn:$PWD; done
    3.打包并提交到flink集群运行

    4.可能hadoop相关的jar包冲突，需要在pom.xml文件中将hadoop-client相关的依赖注释掉，然后重新打包

    5.提交运行后，还可以报错：Could not initialize class org.apache.hadoop.hdfs.protocol.HdfsConstants

        解决方案：
            flink checkpoint集群配置地址与Job的地址不一致导致的问题
            配置文件中的地址 state.backend.fs.checkpointdir:地址
            代码中的地址 env.setStateBackend(new FsStateBackend(地址))
            这两个地方地址要一致
            并且还要修改配置文件中的state.backend:参数
            默认是内存，如果使用hdfs得写成filesystem
            ========
            Flink建议写到配置文件中
            flink-conf.yaml文件
            打开：# state.backend: filesystem
            设置：# state.checkpoints.dir: hdfs://namenode-host:port/flink-checkpoints
            同步到其他节点上去：
                scp flink-conf.yaml node-02:/usr/local/flink-1.9.1/conf
                scp flink-conf.yaml node-03:/usr/local/flink-1.9.1/conf
            重启flink集群
                bin/stop-cluster.sh
                bin/start-cluster.sh

        6.打包、提交到集群去运行，观察其checkpoint目录变化及job被cancel掉后checkpoint是否还在
        7.重启job时，需要指定checkpoint目录，且具体的路径，通过flink web管理界面查看哪级目录存储了checkpoint

        上述是通过web的方式提交flink 作业，生产环境通过命令行提交

         bin/flink run -m node-01:8081 -c com.lei.apitest.c05_project.C06_StateBackendDemo2 -p 4 -s hdfs://node-01:9000/checkpoint/xxx/chk-76 /root/flink-java-1.0-SNAPSHOT.jar node-01

         提交后，通过jps查看有CliFrontend进程，这是提交任务的程序，相当于spark中的spark-submit


 */

public class C06_StateBackendDemo2 {
    public static void main(String[] args) throws Exception {

        // 状态后端数据存储应该存储在分布式文件系统里，便于管理维护；
        // 特别说明，如果提交到集群上运行，不需要setProperty
        //System.setProperty("HADOOP_USER_NAME", "root");
        //System.setProperty("hadoop.home.dir", "/opt/cloudera/parcels/CDH-5.16.1-1.cdh5.16.1.p0.3/bin/");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 只有开启了checkpointing 才会有重启策略
        env.enableCheckpointing(5000); // 开启，检查点周期，单位毫秒；默认是-1，不开启

        // 默认的重启策略是固定延迟无限重启
        //env.getConfig().setRestartStrategy(RestartStrategies.fallBackRestart());
        // 设置固定延迟固定次数重启
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000));

        // 设置状态数据存储的后端，本地文件系统
        // 生产环境将StateBackend保存到分布式文件系统，且flink不建议在代码里写checkpoint目录代码，通过flink配置文件进行指定
        //env.setStateBackend(new FsStateBackend("hdfs://node-01:8020/user/root/flink_checkpoint"));
        //env.setStateBackend(new FsStateBackend(args[1]));

        // 程序异常退出或人为cancel掉，不删除checkpoint的数据
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        DataStreamSource<String> lines = env.socketTextStream(args[0], 7777);
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

        env.execute("C06_StateBackendDemo2");
    }
}
