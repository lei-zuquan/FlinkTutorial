package com.lei.apitest.c02_transformation;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 5:51 下午 2020/6/6
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */

/*
 按多个字段进行分组
 */
public class C06_KeyByDemo3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 直接输入的就是单词
        DataStreamSource<String> lines = env.socketTextStream("localhost", 7777);

        // 辽宁,沈阳,1000
        // 山东,青岛,2000
        // 山东,青岛,2000
        // 山东,烟台,1000
        /*SingleOutputStreamOperator<Tuple3<String, String, Double>> provinceCityMoney = lines.map(new MapFunction<String, Tuple3<String, String, Double>>() {
            @Override
            public Tuple3<String, String, Double> map(String line) throws Exception {
                String[] fields = line.split(",");
                String province = fields[0];
                String city = fields[1];
                double money = Double.parseDouble(fields[2]);
                return Tuple3.of(province, city, money);
            }
        });

        // 元组KeyBy可以用解标
        KeyedStream<Tuple3<String, String, Double>, Tuple> keyed = provinceCityMoney.keyBy(0, 1);

        // 聚合
        SingleOutputStreamOperator<Tuple3<String, String, Double>> sumed = keyed.sum(2);*/

        SingleOutputStreamOperator<C06_OrderBean> provinceCityMoney = lines.map(new MapFunction<String, C06_OrderBean>() {
            @Override
            public C06_OrderBean map(String line) throws Exception {
                String[] fields = line.split(",");
                String province = fields[0];
                String city = fields[1];
                double money = Double.parseDouble(fields[2]);
                return C06_OrderBean.of(province, city, money);
            }
        });

        KeyedStream<C06_OrderBean, Tuple> keyed = provinceCityMoney.keyBy("province", "city");

        SingleOutputStreamOperator<C06_OrderBean> sumed = keyed.sum("money");

        sumed.print();

        env.execute("C04_KeyByDemo1");
    }
}
