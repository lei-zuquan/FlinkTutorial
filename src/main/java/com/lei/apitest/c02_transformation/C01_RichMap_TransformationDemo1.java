package com.lei.apitest.c02_transformation;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/*
 对DataStream进行操作，返回一个新的DataStream

 */
public class C01_RichMap_TransformationDemo1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> lines = env.readTextFile("input_dir/richmap_data.txt");


        // 方式三：传入功能更加强大的RichMapFunction
        // 使用RichXXX_Function，里面含有open，close方法，比如后续读取数据库的前后操作就可以使用open，close
        SingleOutputStreamOperator<String> map = lines.map(
                new RichMapFunction<String, String>() {
            // open，在构造方法之后，map方法执行之前，执行一次，Configuration可以拿到全局配置
            // 用来初始化一下连接，或者初始化或恢复state
            private transient DateTimeFormatter dtf = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                dtf = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
            }

            @Override
            public String map(String value) throws Exception {
                LocalDateTime ldt = LocalDateTime.parse(value, dtf);
                long toEpochSecond = ldt.toEpochSecond(ZoneOffset.of("+8"));

                System.out.println(toEpochSecond);
                long changeSecond = toEpochSecond + 8 * 60 * 60;
                System.out.println(changeSecond);
                return changeSecond + "";
            }
            // 销毁之前，执行一次，通常是做资源释放
            @Override
            public void close() throws Exception {
                super.close();
            }
        });

        System.out.println("==================================");
        map.print();

        env.execute("C01_TransformationDemo1");
    }
}
