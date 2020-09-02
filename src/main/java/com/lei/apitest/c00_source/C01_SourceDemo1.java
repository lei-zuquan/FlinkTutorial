package com.lei.apitest.c00_source;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 10:45 上午 2020/6/6
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */

/*
       并行度为1的source
 */
public class C01_SourceDemo1 {
    public static void main(String[] args) throws Exception {
        // 实时计算，创建一个实时的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 创建抽象的数据集【创建原始的抽象数据集的方法：Source】
        // DataStream是一个抽象的数据集
        DataStream<String> socketTextStream = env.socketTextStream("localhost", 7777);

        int parallelism2 = socketTextStream.getParallelism();
        System.out.println("+++++>" + parallelism2);

        // 将客户端的集后并行化成一个抽象的数据集，通常是用来做测试和实验
        // fromElements是一个有界的数据量，虽然是一个实时计算程序，但是数据处理完，程序就会退出
        //DataStream<Integer> nums = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9);

        // 并行度为1的source
        DataStream<Integer> nums = env.fromCollection(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9));

        // 获取这个DataStream的并行度
        int parallelism = nums.getParallelism();
        System.out.println("=====>" + parallelism);

        SingleOutputStreamOperator<Integer> filtered = nums.filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer integer) throws Exception {
                return integer % 2 == 0;
            }
        }).setParallelism(8);

        int parallelism1 = filtered.getParallelism();
        System.out.println("&&&&&>" + parallelism1);

        filtered.print();

        env.execute("SourceDemo1");

    }
}
