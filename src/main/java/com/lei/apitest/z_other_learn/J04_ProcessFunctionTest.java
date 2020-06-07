package com.lei.apitest.z_other_learn;

import com.lei.domain.J_SensorReading;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: 2020-05-21 16:42
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */

/**
 连续数据中如果温度上升，我们判定为有异常，进行报警提示

 */

/*
    ProcessFunctionAPI(底层API)
    我们之前学习的转换算子是无法访问事件的时间戳信息和水位线信息的。而这在一些应用场景下，极为重要。
    例如：MapFunction这样的map转换算子就无法访问时间戳或者当前事件的事件时间。

    基于此，DataStream API提供了一系列的Low-Level转换算子。可以访问时间戳、watermark以及注册
    定时事件。还可以输出特定的一些事件，例如超时事件等。
    ProcessFunction用来构建事件驱动的应用以及实现自定义的业务逻辑（使用之前的window函数和转换算子
    无法实现）。例如：Flink SQL就是使用Process Function实现的

    Flink 提供了8个ProcessFunction:
        ProcessFunction
        KeyedProcessFunction
        CoProcessFunction
        ProcessJoinFunction
        BroadcastProcessFunction
        KeyedBroadcastProcessFunction
        ProcessWindowFunction
        ProcessAllWindowFunction

 */

public class J04_ProcessFunctionTest {
    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 默认时间语义是：processes time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // watermark产生的事件间隔(每n毫秒)是通过ExecutionConfig.setAutoWatermarkInterval(...)来定义的
        //env.getConfig.setAutoWatermarkInterval(100L) // 默认200毫秒

        // source
        DataStream<String> inputStream = env.socketTextStream("localhost", 7777);

        // Transform操作
        SingleOutputStreamOperator<J_SensorReading> dataStream = inputStream.map(data -> {
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
                    public long extractTimestamp(J_SensorReading j_sensorReading) {
                        return j_sensorReading.timestamp * 1000;
                    }
                });
        SingleOutputStreamOperator<String> processStream =
                dataStream.keyBy(data -> data.id)
                        .process(new J_TempIncreAlert());

        dataStream.print("input data");

        processStream.print("processedStream:");

        env.execute("window test");

    }
}

/*
    富函数（Rich Functions）
    “富函数”是 DataStream API 提供的一个函数类的接口，所有 Flink 函数类都
    有其 Rich 版本。它与常规函数的不同在于，可以获取运行环境的上下文，并拥有一
    些生命周期方法，所以可以实现更复杂的功能。
         RichMapFunction
         RichFlatMapFunction
         RichFilterFunction
         …
    Rich Function 有一个生命周期的概念。典型的生命周期方法有：
         open()方法是 rich function 的初始化方法，当一个算子例如 map 或者 filter被调用之前 open()会被调用。
         close()方法是生命周期中的最后一个调用的方法，做一些清理工作。
         getRuntimeContext()方法提供了函数的 RuntimeContext 的一些信息，例如函数执行的并行度，任务的名字，以及 state 状态

 */
/*
    KeyedProcessFunction

    KeyedProcessFunction 用来操作 KeyedStream。KeyedProcessFunction 会处理流
    的每一个元素，输出为0个、1个或者多个元素。所有的Process Function都继承自
    RichFunction接口，所以都有open()、close()和getRuntimeContext()等方法。而
    KeyedProcessFunction[KEY, IN, OUT] 还额外提供了两个方法：
        precessElement(v: IN, ctx: Context, out: Collect[OUT])
            流中的每一个元素
            都会调用这个方法，调用结果将会放在Collector数据类型中输出。Context
            可以访问元素的时间戳，元素的key，以及TimerService时间服务（可以访问WaterMark）。
            Context 还可以将结果输出到别的流（side outputs)。
        onTimer(timestamp:Long, ctx: OnTimerContext, out: Collector[OUT])
            是一个回调函数(捕捉事件)。当之前注册的定时器触发时调用。参数timestamp为定时器所设定
            的触发的时间戳。Collector为输出结果的集合。OnTimerContext和processElement
            的Context参数一样，提供了上下文的一些信息，例如定时器触发的时间信息（事件时间
            或者处理时间）。

    TimerService和定时器（Timers)
        Context和OnTimerContext所持有的TimerService对象拥有以下方法：
            currentProcessingTime():Long  返回当前处理时间
            currentWatermark():Long       返回当前watermark的时间戳
            registerProcessingTimeTimer(timestamp: Long): Unit会注册当前key的
                processing time的定时器。当processing time到达定时时间时，触发timer.
            registerEventTimeTimer(timestamp:Long):Unit   会注册不前key的event time
                定时器。当水位线大于等于定时器注册的时间时，触发定时器执行回调函数。
            deleteProcessingTimeTimer(timestamp: Long):Unit   删除之前注册处理时间定时器。
                如果没有这个时间戳的定时器，则不执行。
            deleteEventTimeTimer(timestamp: Long):Unit    删除之前注册的事件时间定时器，
                如果没有此时间戳的定时器，则不执行。
       当定时器timer触发时，会执行回调函数onTimer()。注意定时器timer只能在keyed streams上面使用

 */

class J_TempIncreAlert extends KeyedProcessFunction<String, J_SensorReading, String> {


    // 定义一个状态，用来保存上一个数据的温度值。将之前的数据保存到状态里
    ValueState<Double> lastTemp;
    // 定义一个状态，用来保存定时器的时间戳
    ValueState<Long> currentTimer;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 定义一个状态，用来保存上一个数据的温度值。将之前的数据保存到状态里
        lastTemp = getRuntimeContext().getState(new ValueStateDescriptor<Double> ("lastTemp", Double.class));
        // 定义一个状态，用来保存定时器的时间戳
        currentTimer = getRuntimeContext().getState(new ValueStateDescriptor<Long>("currentTimer", Long.class));
    }

    @Override
    public void processElement(J_SensorReading value, Context ctx, Collector<String> collector) throws Exception {
        // 先取出上一个温度值
        Double preTemp = lastTemp.value();
        // 更新温度值
        lastTemp.update(value.temperature);

        Long curTimerTs = currentTimer.value();

        if (preTemp == null || preTemp > value.temperature) {
            // 如果温度下降，或是第一条数据，删除定时器并清空状态
            if (curTimerTs != null) {
                ctx.timerService().deleteProcessingTimeTimer(curTimerTs);
            }

            currentTimer.clear();
        } else if (value.temperature > preTemp && curTimerTs == null){
            // 温度上升且没有设过定时器，则注册定时器
            Long timerTs = ctx.timerService().currentProcessingTime() + 1L;
            ctx.timerService().registerProcessingTimeTimer(timerTs);
            currentTimer.update(timerTs);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
        //super.onTimer(timestamp, ctx, out)
        // 输出报警信息
        out.collect(ctx.getCurrentKey() + " 温度连续上升");
        currentTimer.clear();
    }
}