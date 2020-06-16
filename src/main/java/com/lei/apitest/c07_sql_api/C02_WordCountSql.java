package com.lei.apitest.c07_sql_api;

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
public class C02_WordCountSql {
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

        // 注册表名并指定字段名称
        tEnv.registerDataSet("WordCount", input, "word, counts");

        // 按照单词分组并统计单词次数，然后过虑排序
        String sql = "SELECT word, SUM(counts) as counts FROM WordCount GROUP BY word" +
                " HAVING SUM(counts) >= 2 ORDER BY counts DESC";

        Table table = tEnv.sqlQuery(sql);

        // 将Table表转成DataSet
        DataSet<C01_WordCount> result = tEnv.toDataSet(table, C01_WordCount.class);

        // 输出数据
        result.print();

    }
}
