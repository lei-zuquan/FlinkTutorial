package com.lei.apitest.c03_sink;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 12:17 下午 2020/6/7
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
public class C03_WriteAsTextSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> lines = env.socketTextStream("localhost", 7777);

        lines.writeAsText("out_dir");

        env.execute("C03_WriteAsTextSink");
    }
}
