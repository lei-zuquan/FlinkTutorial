package com.lei.apitest.c07_sql_api;


import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.types.Row;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 10:17 下午 2020/6/16
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
public class C04_KafkaWordCountSQL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.connect(new Kafka()
                .version("universal")
                .topic("json-input")
                .startFromEarliest()
                .property("bootstrap.servers", "node-01:9092,node-02:9092:node-03:9092")
        ).withFormat(new Json().deriveSchema()).withSchema(new Schema()
            .field("name", DataTypes.STRING())
                .field("gender", DataTypes.STRING())
        ).inAppendMode().registerTableSource("kafkaSource");

        Table result = tEnv.scan("kafkaSource")
                .groupBy("gender")
                .select("gender, count(1) as counts");

        tEnv.toRetractStream(result, Row.class).print();

        env.execute("C04_KafkaWordCountSQL");
    }
}
