package com.lei.util;

import com.alibaba.druid.pool.DruidDataSourceFactory;
import com.lei.domain.J_SensorReading;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: 2020-05-25 17:10
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
public class J_MyJdbcUtil extends RichSinkFunction<J_SensorReading> {

    Connection connection = null;
    String sql;

    public J_MyJdbcUtil(String sql) {
        this.sql = sql;
    }

    // 创建连接
    @Override
    public void open(Configuration parameters) throws Exception {
        // super.open(parameters);
        // 获取连接池对象
        DataSource dataSource = DruidDataSourceFactory.createDataSource(J_ConfigurationManager.getProp());
        connection = dataSource.getConnection();
        // 一定要注意druid.properties配置文件中的参数名一定要和上表中的名称相一致，如连接数据库的用户名为username，否则会报错。
    }

    @Override
    public void close() throws Exception {
        //super.close();
        if(connection!=null){
            connection.close();
        }
    }

//    // 反复调用
//    @Override
//    public void invoke(String[] value, Context context) throws Exception {
//        PreparedStatement ps = connection.prepareStatement(sql);
//        System.out.println(Arrays.toString(value));
//        for (int i = 0; i < value.length; i++) {
//            ps.setObject(i + 1, value[i]);
//        }
//
//        ps.executeUpdate();
//    }


    @Override
    public void invoke(J_SensorReading value, Context context) throws Exception {
        PreparedStatement ps = connection.prepareStatement(sql);
        System.out.println(value.toString());
        /*for (int i = 0; i < value.length; i++) {
            ps.setObject(i + 1, value[i]);
        }*/
        ps.setObject(1, value.id);
        ps.setObject(2, value.timestamp);
        ps.setObject(3, value.temperature);

        ps.executeUpdate();
    }
}