package com.lei.sinktest;

import com.lei.domain.J_SensorReading;
import com.lei.util.J_MyRedisUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: 2020-05-25 14:52
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
public class J02_RedisSinkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // source
        DataStreamSource<String> inputStream = env.readTextFile("input_dir/sensor.txt");

        /*
         数据源示例：sensor_1, 1547718205, 27.1
         数据源示例：sensor_2, 1547718206, 28.1
         数据源示例：sensor_3, 1547718207, 29.1
         */
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
        dataStream.addSink(J_MyRedisUtil.getRedisSink());
        /*
            centos redis: redis-cli
            hset channal_sum xiaomi 100
            hset channal_sum huawei 100
            keys *
            get channal_sum
            hgetall channal_sum
            删除所有Key，可以使用Redis的flushdb和flushall命令
         */
        // 把结果存入redis   hset  key:channel_sum   field:  channel   value:  count

        env.execute("redis sink test");
    }
}
