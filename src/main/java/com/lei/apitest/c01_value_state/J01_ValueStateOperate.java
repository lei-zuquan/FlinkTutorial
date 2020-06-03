package com.lei.apitest.c01_value_state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: 2020-06-03 11:24
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */

/*
    作用
      保存一个可以更新和检索的值

    需求
      通过valuestate来实现求取平均值
 */
public class J01_ValueStateOperate {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        env.fromCollection(Arrays.asList(
                new Tuple2<Long, Double>(1L, 3d),
                new Tuple2<Long, Double>(1L, 5d),
                new Tuple2<Long, Double>(1L, 7d),
                new Tuple2<Long, Double>(1L, 4d),
                new Tuple2<Long, Double>(1L, 2d)
        )).keyBy(tuple -> tuple.f0)
                .flatMap(new J_CountWindowAverage())
                .print();

        env.execute("J01_ValueStateOperate");
    }
}

class Avg_Count {
    public Long key;
    public Double value;

    public Avg_Count(Long key, Double value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public String toString() {
        return "Avg_Count{" +
                "key=" + key +
                ", value=" + value +
                '}';
    }
}

/**
 * 定义输入数据类型以及输出的数据类型都是元组
 */
class  J_CountWindowAverage extends RichFlatMapFunction<Tuple2<Long, Double>, Avg_Count> {

    private ValueState<Avg_Count> sum = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        //super.open(parameters);
        ValueStateDescriptor valueStateDescriptor =
                new ValueStateDescriptor<Avg_Count>("average", Avg_Count.class);

        sum = getRuntimeContext().getState(valueStateDescriptor);
    }

    @Override
    public void flatMap(Tuple2<Long, Double> input, Collector<Avg_Count> out) throws Exception {
        Avg_Count tmpCurrentSum = sum.value(); //获取到状态当中的值   第一次的时候，这个值获取不到的
        Avg_Count currentSum;
        if(tmpCurrentSum != null){
            //表示已经获取到了值
            currentSum = tmpCurrentSum;
        }else{
            //没取到值,默认就给一个初始值
            currentSum = new Avg_Count(0L,0d);
        }

        //每次来了一个相同key的数据，就给数据记做出现1次  ==》 数据对应的值全部都累加起来了
        Avg_Count newSum = new Avg_Count(currentSum.key + 1, currentSum.value + input.f1);

        //更新状态值
        sum.update(newSum);

        //求取平均值
        if(newSum.key >=2  ){
            //input._1  得到的是key     newSum._2/newSum._1)  ==》 对应的key，结果的平均值
            out.collect(new Avg_Count(input.f0, newSum.value /newSum.key));
            // sum.clear()
        }
    }
}