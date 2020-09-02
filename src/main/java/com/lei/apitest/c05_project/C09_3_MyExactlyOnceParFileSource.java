package com.lei.apitest.c05_project;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.io.RandomAccessFile;
import java.util.Collections;
import java.util.Iterator;
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
    实现tail -f 功能，每次从上一次读取后的位置继续读

    需要继承RichParallelSourceFunction，同时实现CheckpointedFunction接口

 */
public class C09_3_MyExactlyOnceParFileSource extends RichParallelSourceFunction<Tuple2<String, String>> implements CheckpointedFunction {
    // 记录文件读取路径
    private String path;
    // 记录循环读取文件是否需要被打断
    private boolean flag = true;
    // 记录文件读取偏移量
    private long offset = 0;
    private transient ListState<Long> offsetState;

    public C09_3_MyExactlyOnceParFileSource() {
    }

    public C09_3_MyExactlyOnceParFileSource(String path) {
        this.path = path;
    }

    @Override
    public void run(SourceContext<Tuple2<String, String>> ctx) throws Exception {
        // 获取offsetState历史值
        Iterator<Long> iterator = offsetState.get().iterator();
        while (iterator.hasNext()) {
            offset = iterator.next();
        }

        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();

        // /var/data/0.txt
        RandomAccessFile randomAccessFile = new RandomAccessFile(path + "/" + subtaskIndex + ".txt", "r");
        // 从指定的位置读取数据
        randomAccessFile.seek(offset);

        // todo:由于多线程并发操作offset问题，对offset操作需要添加锁
        final Object lock = ctx.getCheckpointLock();

        while (flag) {
            String line = randomAccessFile.readLine();

            if (line != null) {
                line = new String(line.getBytes("ISO-8859-1"), "UTF-8");
                synchronized (lock) {
                    offset = randomAccessFile.getFilePointer();
                    // 将数据发送出去
                    ctx.collect(Tuple2.of(subtaskIndex + "", line));
                }
            } else {
                TimeUnit.SECONDS.sleep(1000);
            }
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }

    // 定期将指定的状态数据保存到StateBackend中；由JobManager触发
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        // 将历史值清除
        offsetState.clear();
        // 根据最新的状态值
        // offsetState.update(Collections.singletonList(offset));
        offsetState.add(offset);
    }

    // 初始化OperatorState，生命周期方法，构造方法执行后执行一次; 初始化状态或获取历史状态
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // 定义一个状态描述器
        ListStateDescriptor<Long> stateDescriptor = new ListStateDescriptor<Long>(
                "offset-state",
                //TypeInformation.of(new TypeHint<Long>() {})
                //Long.class
                Types.LONG
        );

        // 初始化状态或获取历史状态（OperatorState)
        offsetState = context.getOperatorStateStore().getListState(stateDescriptor);
    }
}
