package com.lei.apitest.z_other_learn.c02_cep;

import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: 2020-06-03 17:15
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
class J_UserLogin  implements Serializable {
    public String ip;
    public String username;
    public String operateUrl;
    public String time;

    public J_UserLogin(String ip, String username, String operateUrl, String time) {
        this.ip = ip;
        this.username = username;
        this.operateUrl = operateUrl;
        this.time = time;
    }

    @Override
    public String toString() {
        return "J_UserLogin{" +
                "ip='" + ip + '\'' +
                ", username='" + username + '\'' +
                ", operateUrl='" + operateUrl + '\'' +
                ", time='" + time + '\'' +
                '}';
    }
}
public class J01_CheckIPChangeWithState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //todo:1、接受socket数据源
        DataStreamSource<String> sourceStream = env.socketTextStream("node-01", 9999);

        //todo:2、数据处理
        // 192.168.52.100,zhubajie,https://icbc.com.cn/login.html,2020-02-12 12:23:45
        sourceStream.map(new J_02MyMapClass())
                .keyBy(t -> t.username)
                .process(new J_02MyProcessClass())
                .print();

        env.execute("J01_CheckIPChangeWithState");
    }
}

class J_02MyMapClass implements MapFunction<String, J_UserLogin> {

    @Override
    public J_UserLogin map(String s) throws Exception {
        String[] split = s.split(",");
        return new J_UserLogin(split[0], split[1], split[2], split[3]);
    }
}

class J_02MyProcessClass extends KeyedProcessFunction<String, J_UserLogin, J_UserLogin> {

    private ListState<J_UserLogin> listState = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        listState = getRuntimeContext().getListState(new ListStateDescriptor<>("listState", J_UserLogin.class));
    }

    @Override
    public void processElement(J_UserLogin value, Context ctx, Collector<J_UserLogin> out) throws Exception {
        // 将数据继续放入到状态收集中
        listState.add(value);

        Iterable<J_UserLogin> j_userLogins = listState.get();
        Iterator<J_UserLogin> iterator = j_userLogins.iterator();
        List<J_UserLogin> list = IteratorUtils.toList(iterator);
        if (list.size() == 2) {
            J_UserLogin one = list.get(0);
            J_UserLogin two = list.get(1);
            if (!one.ip.equals(two.ip)) {
                System.out.println("小兄弟，你的ip换了，赶紧重新登录一下");
            }

//            // 移除第一个ip，保留第二个ip
//            List<J_UserLogin> logins = new ArrayList<>();
//            // 移除第一个ip，保留第二个ip
//            logins.removeAll(Collections.EMPTY_LIST);
//            // list集合当中每次都只保存了最后一条数据
//            logins.add(two);
//            listState.update(logins);

            listState.clear();
            listState.add(two);
        }
        out.collect(value);
    }
}



/*
 * 从socket当中输入数据源

  ~~~
  192.168.52.100,zhubajie,https://icbc.com.cn/login.html,2020-02-12 12:23:45
  192.168.54.172,tangseng,https://icbc.com.cn/login.html,2020-02-12 12:23:46
  192.168.145.77,sunwukong,https://icbc.com.cn/login.html,2020-02-12 12:23:47
  192.168.52.100,zhubajie,https://icbc.com.cn/transfer.html,2020-02-12 12:23:47
  192.168.54.172,tangseng,https://icbc.com.cn/transfer.html,2020-02-12 12:23:48
  192.168.145.77,sunwukong,https://icbc.com.cn/transfer.html,2020-02-12 12:23:49
  192.168.145.77,sunwukong,https://icbc.com.cn/save.html,2020-02-12 12:23:52
  192.168.52.100,zhubajie,https://icbc.com.cn/save.html,2020-02-12 12:23:53
  192.168.54.172,tangseng,https://icbc.com.cn/save.html,2020-02-12 12:23:54
  192.168.54.172,tangseng,https://icbc.com.cn/buy.html,2020-02-12 12:23:57
  192.168.145.77,sunwukong,https://icbc.com.cn/buy.html,2020-02-12 12:23:58
  192.168.52.100,zhubajie,https://icbc.com.cn/buy.html,2020-02-12 12:23:59
  192.168.44.110,zhubajie,https://icbc.com.cn/pay.html,2020-02-12 12:24:03
  192.168.38.135,tangseng,https://icbc.com.cn/pay.html,2020-02-12 12:24:04
  192.168.89.189,sunwukong,https://icbc.com.cn/pay.html,2020-02-12 12:24:05
  192.168.44.110,zhubajie,https://icbc.com.cn/login.html,2020-02-12 12:24:04
  192.168.38.135,tangseng,https://icbc.com.cn/login.html,2020-02-12 12:24:08
  192.168.89.189,sunwukong,https://icbc.com.cn/login.html,2020-02-12 12:24:07
  192.168.38.135,tangseng,https://icbc.com.cn/pay.html,2020-02-12 12:24:10
  192.168.44.110,zhubajie,https://icbc.com.cn/pay.html,2020-02-12 12:24:06
  192.168.89.189,sunwukong,https://icbc.com.cn/pay.html,2020-02-12 12:24:09
  192.168.38.135,tangseng,https://icbc.com.cn/pay.html,2020-02-12 12:24:13
  192.168.44.110,zhubajie,https://icbc.com.cn/pay.html,2020-02-12 12:24:12
  192.168.89.189,sunwukong,https://icbc.com.cn/pay.html,2020-02-12 12:24:15
  ~~~

  * 整理之后的格式如下：

  ~~~
  192.168.145.77,sunwukong,https://icbc.com.cn/login.html,2020-02-12 12:23:47
  192.168.145.77,sunwukong,https://icbc.com.cn/transfer.html,2020-02-12 12:23:49
  192.168.145.77,sunwukong,https://icbc.com.cn/save.html,2020-02-12 12:23:52
  192.168.145.77,sunwukong,https://icbc.com.cn/buy.html,2020-02-12 12:23:58
  192.168.89.189,sunwukong,https://icbc.com.cn/pay.html,2020-02-12 12:24:05
  192.168.89.189,sunwukong,https://icbc.com.cn/login.html,2020-02-12 12:24:07
  192.168.89.189,sunwukong,https://icbc.com.cn/pay.html,2020-02-12 12:24:09
  192.168.89.189,sunwukong,https://icbc.com.cn/pay.html,2020-02-12 12:24:15

  192.168.52.100,zhubajie,https://icbc.com.cn/login.html,2020-02-12 12:23:45
  192.168.52.100,zhubajie,https://icbc.com.cn/transfer.html,2020-02-12 12:23:47
  192.168.52.100,zhubajie,https://icbc.com.cn/save.html,2020-02-12 12:23:53
  192.168.52.100,zhubajie,https://icbc.com.cn/buy.html,2020-02-12 12:23:59
  192.168.44.110,zhubajie,https://icbc.com.cn/pay.html,2020-02-12 12:24:03
  192.168.44.110,zhubajie,https://icbc.com.cn/login.html,2020-02-12 12:24:04
  192.168.44.110,zhubajie,https://icbc.com.cn/pay.html,2020-02-12 12:24:06
  192.168.44.110,zhubajie,https://icbc.com.cn/pay.html,2020-02-12 12:24:12

  192.168.54.172,tangseng,https://icbc.com.cn/login.html,2020-02-12 12:23:46
  192.168.54.172,tangseng,https://icbc.com.cn/transfer.html,2020-02-12 12:23:48
  192.168.54.172,tangseng,https://icbc.com.cn/save.html,2020-02-12 12:23:54
  192.168.54.172,tangseng,https://icbc.com.cn/buy.html,2020-02-12 12:23:57
  192.168.38.135,tangseng,https://icbc.com.cn/pay.html,2020-02-12 12:24:04
  192.168.38.135,tangseng,https://icbc.com.cn/login.html,2020-02-12 12:24:08
  192.168.38.135,tangseng,https://icbc.com.cn/pay.html,2020-02-12 12:24:10
  192.168.38.135,tangseng,https://icbc.com.cn/pay.html,2020-02-12 12:24:13
  ~~~
 */