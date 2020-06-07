package com.lei.apitest.z_other_learn.c01_value_state;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: 2020-06-03 15:53
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
public class J04_ReduceingStateOperate {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.fromCollection(Arrays.asList(
            new Element(1L, 3d),
            new Element(1L, 5d),
            new Element(1L, 7d),
            new Element(2L, 4d),
            new Element(2L, 2d),
            new Element(2L, 6d)
        )).keyBy(value -> value.key)
                .flatMap(new J_CountWithReduceingAverageStage())
                .print();

        env.execute("J04_ReduceingStateOperate");
    }
}


class J_CountWithReduceingAverageStage extends RichFlatMapFunction<Element, Element> {

    // 定义ReducingState
    private ReducingState<Double> reducingState = null;

    // 求取平均值   ==》需要知道每个相同key的数据出现了多少次
    // 定义一个计数器
    Long counter = 0L;

    @Override
    public void open(Configuration parameters) throws Exception {
        ReducingStateDescriptor reduceSum = new ReducingStateDescriptor<Double>("reduceSum", new ReduceFunction<Double>() {

            @Override
            public Double reduce(Double t1, Double t2) throws Exception {
                return t1 + t2;
            }
        }, Double.class);

        //初始化获取mapState对象
        this.reducingState = getRuntimeContext().getReducingState(reduceSum);
    }

    @Override
    public void flatMap(Element element, Collector<Element> out) throws Exception {
        //计数器+1
        counter += 1;
        //添加数据到reducingState
        reducingState.add(element.value);

        out.collect(new Element(element.key,reducingState.get()/counter));
    }
}