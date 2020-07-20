package com.lei.apitest.util;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 10:49 上午 2020/6/13
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
// 自定义高级的RedisSink
public class MyRedisSink extends RichSinkFunction<Tuple3<String, String, String>> {

    private transient Jedis jedis;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // 获取全局的配置参数
        ParameterTool params = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();

        String host = params.getRequired("redis.host");
        String passWord = params.getRequired("redis.pwd");
        int db = params.getInt("redis.db", 0);

        // 获取redis超时连接时间
        jedis = new Jedis(host, 6379, 5000);
        //jedis.auth(passWord);
        jedis.select(db);
    }

    @Override
    public void invoke(Tuple3<String, String, String> value, Context context) throws Exception {
        if (!jedis.isConnected()) {
            jedis.connect();
        }

        jedis.hset(value.f0, value.f1, value.f2);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (jedis != null) {
            jedis.close();
        }
    }
}
