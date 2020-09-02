package com.lei.apitest.c05_project;

import com.lei.apitest.util.FlinkUtilsV1;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.util.concurrent.TimeUnit;

/**
 * @Author:
 * @Date: 2020-06-09 15:06
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
public class C03_AsyncHttpRequest_Test {
    public static void main(String[] args) throws Exception {
        // 输入参数：activity10 group_id_flink node-01:9092,node-02:9092,node-03:9092
        DataStream<String> lines = FlinkUtilsV1.createKafkaStream(args, new SimpleStringSchema());

        //SingleOutputStreamOperator<ActivityBean> beans = lines.map(new C01_DataToActivityBeanFunction());
        SingleOutputStreamOperator<String> result = AsyncDataStream.unorderedWait(
                // 这里的队列不能超过最大队列大小
                lines, new C03_AsyncHttpRequest(), 0, TimeUnit.MILLISECONDS, 10);

        result.print();

        FlinkUtilsV1.getEnv().execute("C03_AsyncHttpRequest_Test");
    }
}
