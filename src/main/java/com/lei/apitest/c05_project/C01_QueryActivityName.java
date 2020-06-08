package com.lei.apitest.c05_project;

import com.lei.apitest.c05_project.domain.ActivityBean;
import com.lei.apitest.util.FlinkUtilsV1;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 10:03 上午 2020/6/8
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
/*

activity10 group_id node-01:9092,node-02:9092,node-03:9092

需要导入mysql驱动：
        <dependency>
            <groupId>mysql</groupId>
            <artifactId>mysql-connector-java</artifactId>
            <version>5.1.44</version>
        </dependency>

1.启动Kafka的topic


2.启动C01_QueryActivityName


3.向Kafka-producer生产数据
u001,A1,2019-09-02 10:10:11,1,北京市
u002,A1,2019-09-02 10:11:11,1,辽宁省
u001,A1,2019-09-02 10:11:11,2,北京市
u001,A1,2019-09-02 10:11:30,3,北京市
u002,A1,2019-09-02 10:12:11,2,辽宁省
u001,A1,2019-09-02 10:10:11,1,北京市
u001,A1,2019-09-02 10:10:11,1,北京市
u001,A1,2019-09-02 10:10:11,1,北京市
u001,A1,2019-09-02 10:10:11,1,北京市

希望得到的数据
u001,新人礼包,2019-09-02 10:10:11,1,北京市
u002,新人礼包,2019-09-02 10:11:11,1,辽宁省

 */
public class C01_QueryActivityName {
    public static void main(String[] args) throws Exception {
        // topic:activity10 分区3，副本2

        DataStream<String> lines = FlinkUtilsV1.createKafkaStream(args, new SimpleStringSchema());

        SingleOutputStreamOperator<ActivityBean> beans = lines.map(new DataToActivityBeanFunction());

        beans.print();

        FlinkUtilsV1.getEnv().execute("C01_QueryActivityName");

    }
}
