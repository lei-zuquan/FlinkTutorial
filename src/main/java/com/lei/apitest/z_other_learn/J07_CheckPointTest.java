package com.lei.apitest.z_other_learn;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: 2020-05-22 17:02
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */

import com.lei.domain.J_SensorReading;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 连续数据中如果温度上升，我们判定为有异常，进行报警提示

 */
public class J07_CheckPointTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 默认时间语义是：processes time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 设置开启CheckPoint机制，间隔6秒；默认是精确一次消费语义：CheckpointingMode.EXACTLY_ONCE
        env.enableCheckpointing(60000);
        // 设置消费语义
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
        // 设置CheckPoint超时时间
        env.getCheckpointConfig().setCheckpointTimeout(100000);
        // 设置CheckPoint失败，是否将Job失败，默认是true
        env.getCheckpointConfig().setFailOnCheckpointingErrors(false);
        // 设置CheckPoint最大同时point数
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 设置两个CheckPoint时，暂停时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(100);
        // 开启一个外部的CheckPoint持久化
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 设置重启策略
        //env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.seconds(300), Time.seconds(10)))
        //env.setStateBackend(new RocksDBStateBackend("hdfs://node-01:8020/user/root/flink_checkpoint"))

        // source
        DataStream<String> inputStream =  env.socketTextStream("node-01", 7777);

        // Transform操作
        DataStream<J_SensorReading> dataStream = inputStream.map(data -> {
            String[] dataArray = data.split(",");
            // 转成String 方便序列化输出
            return new J_SensorReading(dataArray[0].trim(), Long.valueOf(dataArray[1].trim()), Double.valueOf(dataArray[2].trim()));
        })

      /*
       延迟一秒钟上涨水位，这样的操作; 比如：timeWindow是10秒，WaterMark设置成1秒，就是当11秒的数据过来时，才会触发窗口结束
       然后一次会继续上一次的水位时间封装消费10秒窗口数据，同时也得满足水位线延迟1秒
       */
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<J_SensorReading>(Time.seconds(1)) {
          @Override
          public long extractTimestamp(J_SensorReading element) {
              return element.timestamp * 1000;
          }
      });

        DataStream<String> processedStream = dataStream.keyBy(data -> data.id)
                .process(new J_TempIncreAlert07());

        dataStream.print("input data");

        processedStream.print("processedStream:");

        env.execute("window test");
    }
}

class J_TempIncreAlert07 extends KeyedProcessFunction<String, J_SensorReading, String> {
    // 定义一个状态，用来保存上一个数据的温度值。将之前的数据保存到状态里
    ValueState<Double> lastTemp;
    // 定义一个状态，用来保存定时器的时间戳
    ValueState<Long> currentTimer;

    @Override
    public void open(Configuration parameters) throws Exception {
        lastTemp = getRuntimeContext().getState(new ValueStateDescriptor<Double>("lastTemp", Double.class));
        currentTimer = getRuntimeContext().getState(new ValueStateDescriptor<Long>("currentTimer", Long.class));
    }

    @Override
    public void processElement(J_SensorReading value, Context ctx, Collector<String> out) throws Exception {
        // 先取出上一个温度值
        Double preTemp = lastTemp.value();
        // 更新温度值
        lastTemp.update(value.temperature);

        Long curTimerTs = currentTimer.value();

        // 温度下降且没有设定过定时器，则注册定时器
        if (preTemp == null || preTemp > value.temperature) {
            if (curTimerTs != null) {
                ctx.timerService().deleteProcessingTimeTimer(curTimerTs);
            }
            currentTimer.clear();
        } else {

            long timeTs = ctx.timerService().currentProcessingTime() + 100L;
            ctx.timerService().registerProcessingTimeTimer(timeTs);
            currentTimer.update(timeTs);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        // 输出报警信息
        out.collect(ctx.getCurrentKey() + " 温度连续上升");
        currentTimer.clear();
    }
}
