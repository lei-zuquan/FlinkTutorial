package com.lei.apitest.c02_cep;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: 2020-06-03 17:15
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
class J_UserLogin {
    public String ip;
    public String username;
    public String operateUrl;
    public String time;


}
public class J01_CheckIPChangeWithState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //todo:1、接受socket数据源
        DataStreamSource<String> sourceStream = env.socketTextStream("node-01", 9999);

        env.execute("J01_CheckIPChangeWithState");
    }
}
