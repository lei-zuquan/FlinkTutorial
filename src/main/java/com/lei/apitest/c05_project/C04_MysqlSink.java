package com.lei.apitest.c05_project;

import com.lei.apitest.c05_project.domain.ActivityBean;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 10:10 下午 2020/6/9
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */

/*

create table t_activity_counts(
    aid varchar(10),
    event_type int,
    counts int,
    INDEX MultiIdx(aid,event_type)
)

 */
public class C04_MysqlSink extends RichSinkFunction<ActivityBean> {

    private transient Connection connection = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        // 创建MySQL连接
        String url = "jdbc:mysql://localhost:3306/flink_big_data?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true&failOverReadOnly=false";
        String user = "root";
        String password = "1234";

        connection = DriverManager.getConnection(url, user, password);
    }

    @Override
    public void invoke(ActivityBean value, Context context) throws Exception {
        String sql = "INSERT INTO t_activity_counts (aid, event_type, counts) VALUES (?,?,?) ON DUPLICATE KEY UPDATE counts = ?";
        PreparedStatement pstm = connection.prepareStatement(sql);

        try {
            pstm.setString(1, value.aid);
            pstm.setInt(2, value.eventType);
            pstm.setInt(3, value.count);
            pstm.setInt(4, value.count);

            pstm.executeUpdate();
        } finally {
            if (pstm != null) {
                pstm.close();
            }
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        // 关闭连接
        if (connection != null) {
            connection.close();
        }
    }

}
