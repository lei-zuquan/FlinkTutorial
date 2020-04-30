package com.java.flink.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: 2020-04-30 16:35
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
public class C01_WordCount {
    public static void main(String[] args) throws Exception {
        // 创建一个批处理的执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 从文件中读取数据
        String inputPath = "input_dir/hello.txt";
        DataSource<String> inputData = env.readTextFile(inputPath);
        //val inputDataSet: DataSet[String] = env.readTextFile(inputPath)

        // 分词之后做count
//        FlatMapOperator<String, Tuple2<String, Integer>> wordCount =
//                inputData.flatMap(new FlatMapFunction<String,
//                Tuple2<String, Integer>>() {
//            public void flatMap(String line, Collector<Tuple2<String, Integer>> out) {
//                String[] words = line.split(" ");
//                for (String word : words) {
//                    out.collect(new Tuple2<String, Integer>(word, 1));
//                }
//            }
//        });
        FlatMapOperator<String, Tuple2<String, Integer>> wordCount =
            inputData.flatMap( (FlatMapFunction<String, Tuple2<String, Integer>>) (line, out) -> {
                String[] words = line.split(" ");
                List<String> list = Arrays.asList(words);
                list.forEach(word -> out.collect(new Tuple2<>(word, 1)));
            });

        AggregateOperator<Tuple2<String, Integer>> wordsCount = wordCount.groupBy(0).sum(1);

        // 打印输出
        wordsCount.print();
    }
}
