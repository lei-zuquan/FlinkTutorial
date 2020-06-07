package com.lei.apitest.z_other_learn;




/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: 2020-05-22 14:11
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */

import com.lei.domain.J_SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 侧输出流
 低温流，将温度低于XXX度的数据转移至低温流。
 低于32华氏摄氏度

 测试结果如下：
 processed data> SensorReading(sensor_1,1547718199,35.0)
 processed data> SensorReading(sensor_1,1547718199,35.0)
 frezing alert for sensor_1
 frezing alert for sensor_1
 processed data> SensorReading(sensor_1,1547718199,55.0)
 frezing alert for sensor_6

 */

/*
    Emitting to Side Outputs（侧输出）

        大部分的DataStream API的算子的输出是单一输出，也就是某种数据类型的流。
        除了split算子，可以将一条流分成多条流，这些流的数据类型也都相同。process
        function 的 side outputs 功能可以产生多条流，并且这些流的数据类型可以不一样。
        一个side output可以定义为OutputTag[X]对象，X是输出流的数据类型。process
        function可以通过Context对象发射一个事件到一个或者多个side outputs。
 */
public class J05_SideOutputTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 默认时间语义是：processes time
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        // watermark产生的事件间隔(每n毫秒)是通过ExecutionConfig.setAutoWatermarkInterval(...)来定义的
        //env.getConfig.setAutoWatermarkInterval(100L) // 默认200毫秒

        // source
        //DataStreamSource<String> inputStream = env.readTextFile("input_dir/sensor.txt");
        DataStream<String> inputStream = env.socketTextStream("node-01", 7777);

        // Transform操作
        DataStream<J_SensorReading> dataStream = inputStream.map(data -> {
            String[]  dataArray = data.split(",");
            // 转成String 方便序列化输出
            return new J_SensorReading(dataArray[0].trim(), Long.valueOf(dataArray[1].trim()), Double.valueOf(dataArray[2].trim()));
        });

        final OutputTag<String> outputTag = new OutputTag<String>("freezing alert"){};
        SingleOutputStreamOperator<J_SensorReading> processedStream = dataStream.process(new ProcessFunction<J_SensorReading, J_SensorReading>() {
            @Override
            public void processElement(J_SensorReading value, Context ctx, Collector<J_SensorReading> out) throws Exception {
                // 冰点报警，如果小于32F，输出报警信息到侧输出流
                if (value.temperature < 32.0) {
                    ctx.output(outputTag, "frezing alert for " + value.id + "\t" + value.temperature); // 侧输出流
                } else {
                    out.collect(value); // 主流
                }
            }
        });

        //dataStream.print("input data")
        processedStream.print("processed data 正常温度："); // 不是打印侧输出流，而是侧输出流之后的主流
        //processedStream.getSideOutput(new OutputTag[String]("freezing alert")).print() // 打印侧出流

        processedStream.getSideOutput(outputTag).printToErr(" 温度已降到风险位：");

        env.execute("SideOutputTest");
    }
}

