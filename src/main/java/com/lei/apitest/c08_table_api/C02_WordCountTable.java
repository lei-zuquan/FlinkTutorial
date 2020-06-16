package com.lei.apitest.c08_table_api;

import com.lei.apitest.c07_sql_api.C01_WordCount;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: 2020-06-16 9:19
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
public class C02_WordCountTable {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 创建BatchTable上下文环境
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);
        // 模拟测试数据
        DataSource<C01_WordCount> input = env.fromElements(
                new C01_WordCount("Storm", 1L),
                new C01_WordCount("Spark", 1L),
                new C01_WordCount("Flink", 1L),
                new C01_WordCount("Spark", 1L),
                new C01_WordCount("Flink", 1L),
                new C01_WordCount("Flink", 1L) );

        // 通过DataSet创建表
        Table table = tEnv.fromDataSet(input);
        // 调用Table的API进行操作
        Table filtered = table.groupBy("word")  // 分组
                .select("word, counts.sum as counts") // sum
                .filter("counts >= 2")   // 过虑
                .orderBy("counts.desc"); // 排序

        // 将表转成DataSet
        DataSet<C01_WordCount> result = tEnv.toDataSet(filtered, C01_WordCount.class);

        // 输出数据
        result.print();

    }
}
