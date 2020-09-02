package com.lei.apitest.c00_source;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.NumberSequenceIterator;

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
       可以并行的source，即并行度大于1的Source
 */
public class C02_SourceDemo2 {
    public static void main(String[] args) throws Exception {
        // 实时计算，创建一个实时的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //DataStreamSource<Long> nums = env.fromParallelCollection(new NumberSequenceIterator(1, 10), Long.class);
        //DataStreamSource<Long> nums = env.fromParallelCollection(new NumberSequenceIterator(1, 10), TypeInformation.of(Long.TYPE));
        DataStreamSource<Long> nums = env.generateSequence(1, 10);

        // 如果没有设置的话，并行度就是当前机器的逻辑核数
        int parallelism = nums.getParallelism();
        System.out.println("+++++>" + parallelism);

        SingleOutputStreamOperator<Long> filtered = nums.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long integer) throws Exception {
                return integer % 2 == 0;
            }
        }).setParallelism(3);

        int parallelism1 = filtered.getParallelism();
        System.out.println("&&&&&>" + parallelism1);

        filtered.print();

        env.execute("SourceDemo1");

    }
}
