package com.lei.apitest.c02_transformation;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 4:56 下午 2020/6/6
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */

/*
 对DataStream进行操作，返回一个新的DataStream

 */
public class C03_Filter_TransformationDemo1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> nums = env.fromElements(1,2,3,4,5,6,7,8,9);

        SingleOutputStreamOperator<Integer> odd = nums.filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer integer) throws Exception {
                return integer % 2 != 0;
            }
        });

        // lambda表达式
        // SingleOutputStreamOperator<Integer> filtered = nums.filter(i -> i >= 5);
        // 如果lambda表达式比较复杂，需要添加{}，同时，添加return
        SingleOutputStreamOperator<Integer> filtered = nums.filter(i -> {
            return i >= 5;
        });

        // Sink
        filtered.print();


        env.execute("C02_FlatMap_TransformationDemo1");
    }
}
