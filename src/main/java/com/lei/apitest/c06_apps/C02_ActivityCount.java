package com.lei.apitest.c06_apps;

import com.lei.apitest.util.FlinkUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 3:30 下午 2020/6/14
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
/*

// # 创建topic
// kafka-topics --create --zookeeper node-01:2181,node-02:2181,node-03:2181 --replication-factor 2 --partitions 2 --topic activity11
//
// # 创建生产者
// kafka-console-producer --broker-list node-01:9092,node-02:9092,node-03:9092 --topic activity11

各个活动的曝光、点击、参与的人数
各个活动、时间、省份的曝光、点击、参与的次数

将计算好的结果写入到Redis中

1：曝光、2：点击、3：参与
计算活动的点击人数、次数
A1,点击的次数：3, 人数：2

u001,A1,2019-09-02 10:10:11,1,北京市
u002,A1,2019-09-02 10:11:11,1,辽宁省
u001,A1,2019-09-02 10:11:11,2,北京市
u001,A1,2019-09-02 10:11:30,3,北京市
u002,A1,2019-09-02 10:12:11,2,辽宁省
u003,A2,2019-09-02 10:13:11,1,山东省
u003,A2,2019-09-02 10:13:20,2,山东省
u001,A1,2019-09-02 11:11:11,2,北京市

bloom过虑器、bitMap、hyperLogLog(size可以计数)

 */
public class C02_ActivityCount {
    public static void main(String[] args) throws Exception {

        ParameterTool parameters = ParameterTool.fromPropertiesFile(args[0]);

        DataStream<String> lines = FlinkUtils.createKafkaStream(
                parameters,
                SimpleStringSchema.class
        );

        // 将数据进行转换
        // u001,A1,2019-09-02 10:10:11,1,北京市
        SingleOutputStreamOperator<C02_ActBean> beanDataStream = lines.map(new MapFunction<String, C02_ActBean>() {
            @Override
            public C02_ActBean map(String line) throws Exception {
                String[] fields = line.split(",");
                String uid = fields[0];
                String aid = fields[1];
                String time = fields[2];
                String date = time.split(" ")[0];
                Integer type = Integer.parseInt(fields[3]);
                String province = fields[3];

                return C02_ActBean.of(uid, aid, date, type, province, 1);
            }
        });

        // 按照指定的条件进行分组
        // 统计次数
        SingleOutputStreamOperator<C02_ActBean> summed = beanDataStream.keyBy("aid", "time", "type").sum("count");

        summed.print();

        FlinkUtils.getEnv().execute("C02_ActivityCount");
    }
}
