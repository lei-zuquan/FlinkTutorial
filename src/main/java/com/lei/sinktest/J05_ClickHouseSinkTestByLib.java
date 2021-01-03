package com.lei.sinktest;

import com.lei.domain.J_User;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import ru.ivi.opensource.flinkclickhousesink.ClickHouseSink;
import ru.ivi.opensource.flinkclickhousesink.model.ClickHouseClusterSettings;
import ru.ivi.opensource.flinkclickhousesink.model.ClickHouseSinkConst;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @Author:
 * @Date: 2021-01-03 13:06
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */

/*
    进入clickhouse-client
    use default;
    drop table if exists user_table;

    CREATE TABLE default.user_table(id UInt16, name String, age UInt16 ) ENGINE = TinyLog();
 */
public class J05_ClickHouseSinkTestByLib {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        Map<String, String> globalParameters = new HashMap<>();

        // ClickHouse cluster properties
        globalParameters.put(ClickHouseClusterSettings.CLICKHOUSE_HOSTS, "http://node-01:8123/");
        //globalParameters.put(ClickHouseClusterSettings.CLICKHOUSE_USER, ...);
        //globalParameters.put(ClickHouseClusterSettings.CLICKHOUSE_PASSWORD, ...);

        // sink common
        globalParameters.put(ClickHouseSinkConst.TIMEOUT_SEC, "1");
        globalParameters.put(ClickHouseSinkConst.FAILED_RECORDS_PATH, "d:/");
        globalParameters.put(ClickHouseSinkConst.NUM_WRITERS, "2");
        globalParameters.put(ClickHouseSinkConst.NUM_RETRIES, "2");
        globalParameters.put(ClickHouseSinkConst.QUEUE_MAX_CAPACITY, "2");
        globalParameters.put(ClickHouseSinkConst.IGNORING_CLICKHOUSE_SENDING_EXCEPTION_ENABLED, "false");

        // set global paramaters
        ParameterTool parameters = ParameterTool.fromMap(globalParameters);
        env.getConfig().setGlobalJobParameters(parameters);

        env.setParallelism(1);

        // source
        DataStream<String> inputStream = env.socketTextStream("localhost", 7777);

        // Transform 操作
        SingleOutputStreamOperator<String> dataStream = inputStream.map(new MapFunction<String, String>() {
            @Override
            public String map(String data) throws Exception {
                String[] split = data.split(",");
                J_User user = J_User.of(Integer.parseInt(split[0]),
                        split[1],
                        Integer.parseInt(split[2]));
                return J_User.convertToCsv(user);
            }
        });

        // create props for sink
        Properties props = new Properties();
        props.put(ClickHouseSinkConst.TARGET_TABLE_NAME, "default.user_table");
        props.put(ClickHouseSinkConst.MAX_BUFFER_SIZE, "10000");
        ClickHouseSink sink = new ClickHouseSink(props);
        dataStream.addSink(sink);
        dataStream.print();

        env.execute("clickhouse sink test");
    }
}
