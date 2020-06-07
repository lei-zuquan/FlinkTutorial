package com.lei.apitest.z_other_learn;

import com.lei.domain.J_SensorReading;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: 2020-05-21 11:17
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
public class J03_WindowTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 默认时间语义是：processes time
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        // watermark产生的事件间隔(每n毫秒)是通过ExecutionConfig.setAutoWatermarkInterval(...)来定义的
        env.getConfig().setAutoWatermarkInterval(100L);  // 默认200ms

        // 2、从文件中读取数据
        //DataStreamSource<String> inputStream = env.readTextFile("input_dir/sensor.txt");
        DataStreamSource<String> inputStream = env.socketTextStream("localhost", 7777);

        // Transform操作
        SingleOutputStreamOperator<J_SensorReading> dataStream = inputStream.map(data -> {
            String[] dataArray = data.split(",");
            // 转成String 方便序列化输出
            return new J_SensorReading(dataArray[0].trim(), Long.valueOf(dataArray[1].trim()), Double.valueOf(dataArray[2].trim()));
        })

                //.assignAscendingTimestamps(_.timestamp * 1000)
                //.assignTimestampsAndWatermarks(new MyAssigner())
                // 经典写法是：BoundedOutOfOrdernessTimestampExtractor

                /*
                 延迟一秒钟上涨水位，这样的操作; 比如：timeWindow是10秒，WaterMark设置成1秒，就是当11秒的数据过来时，才会触发窗口结束
                 然后一次会继续上一次的水位时间封装消费10秒窗口数据，同时也得满足水位线延迟1秒
                */
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<J_SensorReading>(Time.seconds(2)) {

                    @Override
                    public long extractTimestamp(J_SensorReading element) {
                        return element.timestamp * 1000;
                    }
                });

        SingleOutputStreamOperator<J_SensorReading> minTempPerWindowStream = dataStream.map(data -> new J_SensorReading(data.id, data.timestamp, data.temperature))
                .keyBy(data -> data.id)
                //.timeWindow(Time.seconds(10)) // 开时间窗口 // 统计10秒内的最小温度
                //.timeWindow(Time.seconds(15), Time.seconds(5)) // 统计15秒内的最小温度，隔5秒输出一次；左闭右开【）包含开始，不包含结束
                // timeWindow底层调用的还是window
                .window(SlidingEventTimeWindows.of(Time.seconds(15), Time.seconds(5), Time.hours(-8)))
                .reduce(new ReduceFunction<J_SensorReading>() {
                    @Override
                    public J_SensorReading reduce(J_SensorReading data1, J_SensorReading data2) throws Exception {
                        Long minTime = data1.temperature < data2.temperature ? data1.timestamp : data2.timestamp;
                        double minTure = data1.temperature < data2.temperature ? data1.temperature : data2.temperature;

                        return new J_SensorReading(data1.id, minTime, minTure);
                    }
                });// 用reduce做增量聚合

        minTempPerWindowStream.printToErr("10秒内最小温度min temp： ");

        dataStream.print("input data");

        dataStream.keyBy(data -> data.id)
                .process(new J_MyProcess());

        env.execute("windowTest");
    }
}

class J_MyProcess extends KeyedProcessFunction<String, J_SensorReading, String> {

    @Override
    public void processElement(J_SensorReading j_sensorReading, Context context, Collector<String> collector) throws Exception {
        context.timerService().registerEventTimeTimer(2000L);
    }
}