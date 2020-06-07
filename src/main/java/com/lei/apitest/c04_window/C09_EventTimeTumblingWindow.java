package com.lei.apitest.c04_window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 3:18 下午 2020/6/7
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */

// 使用EventTime, 划分滚动窗口
// 如果使用的是并行的Source,例如KafkaSource，创建Kafka的Topic时有多个分区
// 每一个Source的分区都要满足触发的条件，整个窗口才会触发; 与数据分组不关
// 如果EventTime有数据缓慢到来，则会出现数据积压的问题；
// EventTime使用场景就是数据源源不断地产生。

public class C09_EventTimeTumblingWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置EventTime作为时间标准
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties props = new Properties();

        // 指定Kafka的Broker地址
        props.setProperty("bootstrap.servers", "node-01:9092,node-02:9092,node-03:90902");
        // 提定组ID
        props.setProperty("group.id", "gwc10");
        // 如果没有记录偏移量，第一次从开始消费
        props.setProperty("auto.offset.reset", "earliest");
        // kafka的消费者不自动提交偏移量，默认kafka自动提交offset,且保存在__consumer_offsets
        // props.setProperty("enable.auto.commit", "false");

        // kafkaSource
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>(
                "wc10",
                new SimpleStringSchema(), // 序列化与反序列化方式
                props);

        // Source
        DataStream<String> kafkaLines = env.addSource(kafkaSource);

        // 1000,spark,3
        // 1100,hadoop,2
        // 仅仅只是提取时间字段，不会改变数据的样式
        SingleOutputStreamOperator<String> lines = env.socketTextStream("localhost", 7777)
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(0)) {
                    // 将数据中的时间字段提取出来，然后转成long类型
                    @Override
                    public long extractTimestamp(String line) {
                        String[] fields = line.split(",");
                        return Long.parseLong(fields[0]);
                    }
                });

        // 提取时间字段

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndCount = lines.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] fields = value.split(",");
                String word = fields[1];
                Integer count = Integer.parseInt(fields[2]);
                return Tuple2.of(word, count);
            }
        });

        // 先分组，再划分窗口
        KeyedStream<Tuple2<String, Integer>, Tuple> keyed = wordAndCount.keyBy(1);

        // 划分窗口
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> window = keyed.window(TumblingEventTimeWindows.of(Time.seconds(5)));

        SingleOutputStreamOperator<Tuple2<String, Integer>> summed = window.sum(1);

        summed.print();

        env.execute("C08_EventTimeSessionWindow");
    }
}
