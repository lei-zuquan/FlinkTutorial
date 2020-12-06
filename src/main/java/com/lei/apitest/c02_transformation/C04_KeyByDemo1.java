package com.lei.apitest.c02_transformation;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 5:51 下午 2020/6/6
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */

/*
 keyBy是shuffle算子
 在Flink中叫redistrute
 */
public class C04_KeyByDemo1 {

    private static Logger LOG = LoggerFactory.getLogger(C04_KeyByDemo1.class);

    public static void main(String[] args) throws Exception {
        LOG.info("This message contains {} placeholders. {}", 2, "Yippie");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 直接输入的就是单词
        DataStreamSource<String> words = env.socketTextStream("localhost", 7777);

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = words.map(w -> Tuple2.of(w, 1)).returns(Types.TUPLE(Types.STRING, Types.INT));
        
        // 在java，认为元素是一个特殊的集合，脚标是从0开始；因为Flink底层源码是java编写的
        KeyedStream<Tuple2<String, Integer>, Tuple> keyed = wordAndOne.keyBy(0);

        keyed.print();

        env.execute("C04_KeyByDemo1");
    }
}
