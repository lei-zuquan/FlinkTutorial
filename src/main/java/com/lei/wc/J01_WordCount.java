package com.lei.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 10:36 下午 2020/5/13
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */

public class J01_WordCount {
    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 从文件中读取数据
        String inputPath = "input_dir/hello.txt";
        DataSet<String> inputDataSet = env.readTextFile(inputPath);
        // 创建DataSet，这里我们的输入是一行一行的文本
        /*DataSet<String> inputDataSet = env.fromElements(
                "Flink Spark Storm",
                "Flink Flink Flink",
                "Spark Spark Spark",
                "Storm Storm Storm"
        );*/

        AggregateOperator<Tuple2<String, Integer>> counts = inputDataSet.flatMap(
            (String line, Collector<Tuple2<String, Integer>> collector) -> {
                String[] words = line.split(" ");
                for (String word : words) {
                    collector.collect(new Tuple2<>(word, 1));
                }
            }
        ).returns(Types.TUPLE(Types.STRING, Types.INT)).groupBy(0).sum(1);

        counts.print();
    }
}
