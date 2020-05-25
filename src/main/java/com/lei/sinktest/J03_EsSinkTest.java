package com.lei.sinktest;

import com.lei.domain.J_SensorReading;
import com.lei.util.J_MyEsUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: 2020-05-25 16:01
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
public class J03_EsSinkTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // source
        DataStream<String> inputStream = env.readTextFile("input_dir/sensor.txt");

        SingleOutputStreamOperator<J_SensorReading> dataStream = inputStream.map(
                (String data) -> {
                    String[] dataArray = data.split(",");
                    // 转成String 方便序列化输出
                    return new J_SensorReading(dataArray[0].trim(),
                            Long.valueOf(dataArray[1].trim()),
                            Double.valueOf(dataArray[2].trim()));
                }
        ).returns(J_SensorReading.class);

        // sink
        dataStream.addSink(J_MyEsUtil.getElasticSearchSink("sensor"));
        dataStream.print();

        env.execute("es sink test");
    }
}
