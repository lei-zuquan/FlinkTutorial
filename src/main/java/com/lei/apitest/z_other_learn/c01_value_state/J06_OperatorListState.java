package com.lei.apitest.z_other_learn.c01_value_state;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: 2020-06-03 16:21
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */

/*
  需求
    实现每两条数据进行输出打印一次，不用区分数据的key
    这里使用ListState实现
 */
/**
 * 实现每两条数据进行输出打印一次，不用区分数据的key
 */
public class J06_OperatorListState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<MyKeyValue> sourceStream = env.fromCollection(Arrays.asList(
                new MyKeyValue("spark", 3),
                new MyKeyValue("hadoop", 5),
                new MyKeyValue("hive", 7),
                new MyKeyValue("flume", 9)
        ));

        sourceStream.addSink(new J_OperateTaskState()).setParallelism(1);


        env.execute("J06_OperatorListState");
    }
}

class MyKeyValue {
    public String key;
    public Integer value;

    public MyKeyValue(String key, Integer value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public String toString() {
        return "MyKeyValue{" +
                "key='" + key + '\'' +
                ", value=" + value +
                '}';
    }
}


class J_OperateTaskState implements SinkFunction<MyKeyValue> {
    // 定义一个list 用于我们每两条数据打印一下
    private List<MyKeyValue> listBuffer = new ArrayList<>();

    @Override
    public void invoke(MyKeyValue value, Context context) throws Exception {
        listBuffer.add(value);

        if(listBuffer.size() ==2){
            System.out.println(listBuffer);

            // 清空state状态   每隔两条数据，打印一下之后，清空状态
            listBuffer.clear();
        }
    }
}