package com.lei.apitest.c05_project;

import com.lei.apitest.c05_project.domain.ActivityBean;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 10:20 上午 2020/6/8
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
public class C01_DataToActivityBeanFunction extends RichMapFunction<String, ActivityBean> {

    private Connection connection = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // 创建MySQL连接
        // 这里不应该对异常进行捕获，让Flink自行处理，比如重启之类的
        // 如果捕获异常了，则Flink无法捕获到该异常
        String url = "jdbc:mysql://localhost:3306/flink_big_data?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true&failOverReadOnly=false";
        String user = "root";
        String password = "1234";
        connection = DriverManager.getConnection(url, user, password);
    }

    @Override
    public ActivityBean map(String line) throws Exception {
        String[] fields = line.split(",");

        String uid = fields[0];
        String aid = fields[1];

        // 根据aid作为查询条件查询出name
        // 最好使用简单的关联查询，MySQL也可以进行关联查询
        PreparedStatement preparedStatement = connection.prepareStatement("SELECT name FROM t_activities WHERE a_id = ?");
        preparedStatement.setString(1, aid);
        ResultSet resultSet = preparedStatement.executeQuery();
        String name = null;
        while (resultSet.next()) {
            name = resultSet.getString(1);
        }

        String time = fields[2];
        int eventType = Integer.parseInt(fields[3]);
        String province = fields[4];

        return ActivityBean.of(uid, aid, name, time, eventType, province);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) {
            connection.close();
        }
    }
}
