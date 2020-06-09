package com.lei.apitest.c05_project;

import com.alibaba.druid.pool.DruidDataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Supplier;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: 2020-06-09 11:49
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
public class C03_AsyncMySQLRequest extends RichAsyncFunction<String, String> {

    private transient DruidDataSource dataSource;

    private transient ExecutorService executorService;

    @Override
    public void open(Configuration parameters) throws Exception {
        //super.open(parameters);
        executorService = Executors.newFixedThreadPool(20);

        dataSource = new DruidDataSource();
        dataSource.setDriverClassName("com.mysql.jdbc.Driver");
        dataSource.setUsername("root");
        dataSource.setPassword("1234");
        dataSource.setUrl("jdbc:mysql://localhost:3306/bigdata?characterEncoding=UTF-8");
        dataSource.setInitialSize(5);
        dataSource.setMinIdle(10);
        dataSource.setMaxActive(20);
    }

    @Override
    public void close() throws Exception {
        super.close();
        executorService.shutdown();
    }

    @Override
    public void asyncInvoke(String id, final ResultFuture<String> resultFuture) throws Exception {
        Future<String> future = executorService.submit(() -> {
            return queryFromMySql(id);
        });

        CompletableFuture.supplyAsync(new Supplier<String>() {
            @Override
            public String get() {
                try {
                    return future.get();
                } catch (Exception e){
                    return null;
                }
            }
        }).thenAccept((String dbResult) -> {
            resultFuture.complete(Collections.singleton(dbResult));
        });
    }

    private String queryFromMySql(String param) throws SQLException {
        String sql = "SELECT id, name FROM t_data WHERE id = ?";
        String result = null;

        Connection connection = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            connection = dataSource.getConnection();
            stmt = connection.prepareStatement(sql);
            stmt.setString(1, param);
            rs = stmt.executeQuery();
            while (rs.next()) {
                result = rs.getString("name");
            }
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (stmt != null) {
                stmt.close();
            }
            if (connection != null) {
                connection.close();
            }
        }

        if (result != null) {
            // 放入缓存中
        }
        return result;
    }
}
