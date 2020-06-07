package com.lei.apitest.z_other_learn.c01_value_state;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: 2020-06-03 16:03
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
public class J05_AggregrateStateOperate {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.fromCollection(Arrays.asList(
                new Element(1L, 3d),
                new Element(1L, 5d),
                new Element(1L, 7d),
                new Element(2L, 4d),
                new Element(2L, 2d),
                new Element(2L, 6d)
        )).keyBy(t -> t.key)
                .flatMap(new J_AggregrageState())
                .print();

        env.execute("J05_AggregrateStateOperate");
    }
}


class J_AggregrageState extends RichFlatMapFunction<Element, Tuple2<Long, String>> {

    //定义AggregatingState
    private AggregatingState<Double, String> aggregateTotal = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        /**
         * name: String,
         * aggFunction: AggregateFunction[IN, ACC, OUT],
         * stateType: Class[ACC]
         */
        AggregatingStateDescriptor aggregateStateDescriptor =
                new AggregatingStateDescriptor<Double, String, String>(
                        "aggregateState", new AggregateFunction<Double, String, String>() {
                    //创建一个初始值
                    @Override
                    public String createAccumulator() {
                        return "Contains";
                    }

                    @Override
                    public String add(Double value, String accumulator) {
                        /*if ("Contains".equals(accumulator)) {
                            accumulator += value;
                        }*/

                        return accumulator + "and" + value;
                    }

                    @Override
                    public String getResult(String accumulator) {
                        return accumulator;
                    }

                    @Override
                    public String merge(String a, String b) {
                        return a + "and" + b;
                    }

        }, String.class);

        aggregateTotal = getRuntimeContext().getAggregatingState(aggregateStateDescriptor);
    }

    @Override
    public void flatMap(Element element, Collector<Tuple2<Long, String>> out) throws Exception {
        aggregateTotal.add(element.value);
        out.collect(new Tuple2<Long, String>(element.key, aggregateTotal.get()));
    }
}