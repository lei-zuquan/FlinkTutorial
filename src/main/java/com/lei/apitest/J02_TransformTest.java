package com.lei.apitest;

import com.lei.domain.J_SensorReading;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: 2020-05-21 9:46
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
public class J02_TransformTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // 设置全局的并行度

        // map
        // flatMap
        // filter
        // keyBy: 逻辑地将一个流拆分成不相交的分区，每个分区包含具有相同key的元素
        //      DataStream -> KeyedSteam(内部以Hash的形式实现的，可能数据倾斜)
        // 滚动聚合算子（Rolling Aggregation）
        //      sum()
        //      min()
        //      max()
        //      minBy()
        //      maxBy()

        // 1.基本转换算子和简单聚合算子
        DataStreamSource<String> streamFromFile = env.readTextFile("input_dir/sensor.txt");
        //streamFromFile.print();

        SingleOutputStreamOperator<J_SensorReading> dataStream = streamFromFile.map(
                (String data) -> {
                String[] dataArray = data.split(",");
                return new J_SensorReading(dataArray[0].trim(), Long.valueOf(dataArray[1].trim()), Double.valueOf(dataArray[2].trim()));
            }
        ).returns(J_SensorReading.class)
            .keyBy(data -> data.id)
            //.reduce()
            //.sum(2)
            // 输出当前传感器最新的温度+10，而时间戳是上一次数据的时间+1
            .reduce((x, y) -> new J_SensorReading(x.id, x.timestamp + 1, y.temperature + 10));

        // 2.多流转换算子
        // 分流split
        SplitStream<J_SensorReading> splitStream = dataStream.split( (J_SensorReading sensorReading) -> {
            List<String> list = new ArrayList<>();
            if (sensorReading.temperature > 30) {
                list.add("high");
            } else {
                list.add("low");
            }

            return list;
        });

        DataStream<J_SensorReading> high  = splitStream.select("high");
        DataStream<J_SensorReading> low = splitStream.select("low");
        DataStream<J_SensorReading> all = splitStream.select("high", "low");

        /*high.print("high");
        low.print("low");
        all.print("all");*/


        // 合并两条流
        // connect合并两条流，两条流数据类型不一致也可以；最多2条流
        SingleOutputStreamOperator<Tuple2<String, Double>> warningStream = high.map(new MapFunction<J_SensorReading, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(J_SensorReading data) throws Exception {
                return new Tuple2(data.id, data.temperature);
            }
        });

        ConnectedStreams<Tuple2<String, Double>, J_SensorReading> connectedStream = warningStream.connect(low);

        SingleOutputStreamOperator<Tuple2<String, String>> coMapDataStream = connectedStream.map(new CoMapFunction<Tuple2<String, Double>, J_SensorReading, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map1(Tuple2<String, Double> warningData) throws Exception {
                return new Tuple2<>(warningData._1, "warning");
            }

            @Override
            public Tuple2<String, String> map2(J_SensorReading lowData) throws Exception {
                return new Tuple2<>(lowData.id, "healthy");
            }
        });

        //coMapDataStream.print();

        // union合并流
        // 多条流数据类型要求一致，数据结构必须一样
        DataStream<J_SensorReading> unionStream = high.union(low);
        unionStream.print("union");

        /**
         * Connect与Union区别
         * 1、Union之前两个流的类型必须是一样， Connect可以不一样，在之后的coMap中再去调整成为一样的。
         * 2、Connect只能操作两个流，Union可以操作多个。
         */

        // 函数类
        //dataStream.filter(new J_MyFilter()).print();

        env.execute("J02_TransformTest");
    }
}

class J_MyFilter implements FilterFunction<J_SensorReading> {

    @Override
    public boolean filter(J_SensorReading j_sensorReading) throws Exception {
        return j_sensorReading.id.startsWith("sensor_1");
    }
}
