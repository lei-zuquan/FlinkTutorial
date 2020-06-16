package com.lei.apitest.c07_sql_api;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 10:34 下午 2020/6/16
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
public class C05_UDFSQL {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 注册一个可以Cache的文件，通过网络发送给TaskManager
        env.registerCachedFile("ip.txt", "ip-rules");

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 106.121.4.252
        // 42.57.88.186
        DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 7777);

        tableEnv.registerDataStream("t_lines", socketTextStream, "ip");

        // 注册自定义函数，是一个UDF，输入一个IP地址，返回Row<省、市>
        tableEnv.registerFunction("ipLocation", new C05_IpLocation());

        // tableEnv.registerFunction("split", new Split("\\W+"));
        Table table = tableEnv.sqlQuery(
                "SELECT ip, ipLocation(ip) FROM t_lines");

        tableEnv.toAppendStream(table, Row.class).print();

        env.execute("C05_UDFSQL");

    }
}
