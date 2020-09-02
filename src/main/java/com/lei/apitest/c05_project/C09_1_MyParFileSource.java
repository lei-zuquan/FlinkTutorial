package com.lei.apitest.c05_project;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.io.RandomAccessFile;
import java.util.concurrent.TimeUnit;

/**
 * @Author:
 * @Date: 2020-06-12 8:44
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */

/*
    自定义可以并行的source
    需要继承RichParallelSourceFunction，重写run与cancel方法
 */
public class C09_1_MyParFileSource extends RichParallelSourceFunction<Tuple2<String, String>> {
    private String path;
    private boolean flag = true;

    public C09_1_MyParFileSource() {
    }

    public C09_1_MyParFileSource(String path) {
        this.path = path;
    }

    @Override
    public void run(SourceContext<Tuple2<String, String>> ctx) throws Exception {
        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();

        // /var/data/0.txt
        RandomAccessFile randomAccessFile = new RandomAccessFile(path + "/" + subtaskIndex + ".txt", "r");

        while (flag) {
            String line = randomAccessFile.readLine();

            if (line != null) {
                line = new String(line.getBytes("ISO-8859-1"), "UTF-8");
                // 将数据发送出去
                ctx.collect(Tuple2.of(subtaskIndex + "", line));
            } else {
                TimeUnit.SECONDS.sleep(1000);
            }
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }
}
