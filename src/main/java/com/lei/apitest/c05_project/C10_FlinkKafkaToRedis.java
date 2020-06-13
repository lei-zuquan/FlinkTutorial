package com.lei.apitest.c05_project;

import com.lei.apitest.util.FlinkUtils;
import com.lei.apitest.util.FlinkUtilsV1;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 9:39 上午 2020/6/13
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */

/*

--group.id g_10 --topics s1,s2

 */
public class C10_FlinkKafkaToRedis {
    public static void main(String[] args) throws Exception {
        /*ParameterTool parameters = ParameterTool.fromArgs(args);
        */

        // 上述方式可以将传入的配置参数读取，但是在实际生产环境最佳放在指定的文件中
        // 而不是放在maven 项目的resources中，因为打完包后，想要修改就不太方便了
        // Flink 官方最佳实践
        ParameterTool parameters = ParameterTool.fromPropertiesFile(args[0]);
        DataStream<String> lines = FlinkUtils.createKafkaStream(parameters, SimpleStringSchema.class);

        lines.print();

        FlinkUtils.getEnv().execute("C10_FlinkKafkaToRedis");

    }
}
