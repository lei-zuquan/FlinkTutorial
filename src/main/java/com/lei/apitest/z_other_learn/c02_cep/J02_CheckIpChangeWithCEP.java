package com.lei.apitest.z_other_learn.c02_cep;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

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

        DataStreamSource<String> sourceStream = env.socketTextStream("localhost", 7777);

        KeyedStream<J_UserLogin, String> keyedStream =
                sourceStream.map(new J02_MyMapCep()).keyBy(t -> t.username);

        Pattern<J_UserLogin, J_UserLogin> pattern = Pattern.<J_UserLogin>begin("start")
                .where(new IterativeCondition<J_UserLogin>() {
                    @Override
                    public boolean filter(J_UserLogin j_userLogin, Context<J_UserLogin> context) throws Exception {
                        return j_userLogin.username != null;
                    }
                })
                .next("second")
                .where(new IterativeCondition<J_UserLogin>() {
                    @Override
                    public boolean filter(J_UserLogin j_userLogin, Context<J_UserLogin> context) throws Exception {
                        boolean flag = false;
                        Iterator<J_UserLogin> firstValues = context.getEventsForPattern("start").iterator();
                        // 满足第一个条件
                        // 第一个条与第二个条件进行比较判断

                        while (firstValues.hasNext()) {
                            J_UserLogin user = firstValues.next();
                            if (!user.ip.equals(j_userLogin.ip)) {
                                flag = true;
                            }
                        }
                        return flag; // 如果flag是true，表示满足我们的条件的数据 两个ip不相等，ip发生了变换
                    }
                }).within(Time.seconds(120));

        // 将规则应用到流数据上面去
        PatternStream<J_UserLogin> patterStream = CEP.pattern(keyedStream, pattern);

        // 从patterStream里面获取满足条件的数据
        patterStream.select(new J02_MyPatternSelectFunction()).print();

        env.execute("J02_CheckIpChangeWithCEP");
    }
}

class J02_MyMapCep implements MapFunction<String, J_UserLogin> {

    @Override
    public J_UserLogin map(String s) throws Exception {
        String[] split = s.split(",");
        return new J_UserLogin(split[0], split[1], split[2], split[3]);
    }
}

class J02_MyPatternSelectFunction implements PatternSelectFunction<J_UserLogin, J_UserLogin> {
    @Override
    public J_UserLogin select(Map<String, List<J_UserLogin>> map) throws Exception {
        // 获取Pattern名称为start的事件
        Iterator<J_UserLogin> startIterator = map.get("start").iterator();
        if (startIterator.hasNext()) {
            System.out.println("满足start模式中的数据：" + startIterator.next());
        }

        // 获取Pattern名称为second的事件
        Iterator<J_UserLogin> secondIterator = map.get("second").iterator();
        J_UserLogin user = null;

        if (secondIterator.hasNext()) {
            user = secondIterator.next();
            System.out.println("满足second模式中的数据：" + user);
        }

        return user;
    }
}