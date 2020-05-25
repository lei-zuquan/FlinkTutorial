package com.lei.sinktest;

import com.lei.domain.J_SensorReading;
import com.lei.util.J_MyJdbcUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: 2020-05-25 17:21
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
public class J04_JdbcSinkTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // source
        DataStream<String> inputStream = env.readTextFile("input_dir/sensor.txt");

        // Transform操作
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
        // dataStream.addSink(new MyJdbcSink())


        J_MyJdbcUtil jdbcSink = new J_MyJdbcUtil("insert into temperatures values(?,?,?)");
        dataStream.addSink(jdbcSink);

        TimeUnit.SECONDS.sleep(60);
        env.execute("jdbc sink test");
    }
}
