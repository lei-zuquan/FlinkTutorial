package com.lei.apitest.c03_sink;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 11:51 上午 2020/6/7
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
// 在 flink 中 print 属于一种sink
public class C01_PrintSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("localhost", 7777);

        // print时显示的数字哪里来的，代表什么含义：PrintSinkOutputWriter.java
        // completedPrefix += (subtaskIndex + 1);
        // completedPrefix += "> ";

        // print是测试时使用，生产环境与其他存储介质结合使用
        lines.print("res").setParallelism(2);

        env.execute("C01_PrintSink");
    }
}
