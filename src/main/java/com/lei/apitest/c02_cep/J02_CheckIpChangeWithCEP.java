package com.lei.apitest.c02_cep;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: 2020-06-04 19:22
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
public class J02_CheckIpChangeWithCEP {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> sourceStream = env.socketTextStream("node-01", 9999);

        KeyedStream<J_UserLogin, String> keyedStream =
                sourceStream.map(new J02_MyMapCep()).keyBy(t -> t.username);




        env.execute("J02_CheckIpChangeWithCEP");
    }
}

class J02_MyMapCep implements MapFunction<String, J_UserLogin> {

    @Override
    public J_UserLogin map(String s) throws Exception {
        String[] split = s.split(",");
        return new J_UserLogin(split[0], split[1], split[3], split[4]);
    }
}
