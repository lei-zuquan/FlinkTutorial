package com.lei.apitest.c08_table_api;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: 2020-06-16 9:58
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
public class C03_TumblingEventTimeWindowsTable {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 1000,u1,p1,5
        // 2000,u1,p1,5
        // 2000,u2,p1,3
        // 3000,u1,p1,5
        // 9999,u2,p1,3
        // 19999,u2,p1,5
        DataStreamSource<String> socketTextStream = env.socketTextStream("node-01", 7777);
        SingleOutputStreamOperator<Row> rowDataStream = socketTextStream.map(new MapFunction<String, Row>() {
            @Override
            public Row map(String line) throws Exception {
                String[] fields = line.split(",");
                long time = Long.parseLong(fields[0]);
                String uid = fields[1];
                String pid = fields[2];
                Double money = Double.parseDouble(fields[3]);

                return Row.of(time, uid, pid, money);
            }
        }).returns(Types.ROW(Types.LONG, Types.STRING, Types.STRING, Types.DOUBLE));

        SingleOutputStreamOperator<Row> waterMarkRow = rowDataStream.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<Row>(Time.seconds(0)) {
                    @Override
                    public long extractTimestamp(Row row) {
                        return (long) row.getField(0);
                    }
                }
        );

        tableEnv.registerDataStream(
                "t_orders", waterMarkRow, "atime, uid, pid, money, rowtime.rowtime");

        // [0,10000]
        // [0,9999]
        // win.start 0
        // win.end   10000
        // win.rowtime 9999
        Table table = tableEnv.scan("t_orders")
                .window(Tumble.over("10.seconds").on("rowtime").as("win")) // define window
                .groupBy("uid, win") // group by key and window
                .select("uid, win.start, win.end, win.rowtime, money.sum as total");

        tableEnv.toAppendStream(table, Row.class).print();

        env.execute();
    }
}
