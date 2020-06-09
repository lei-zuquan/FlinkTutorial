package com.lei.apitest.c05_project;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lei.apitest.c05_project.domain.ActivityBean;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.util.EntityUtils;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.Supplier;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: 2020-06-09 12:06
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
public class C03_AsyncHttpRequest extends RichAsyncFunction<String, String> {

    private transient CloseableHttpAsyncClient httpClient = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        //super.open(parameters);
        // 初始化异步的HttpClient
        RequestConfig requestConfig = RequestConfig.custom()
                .setSocketTimeout(3000)
                .setConnectTimeout(3000)
                .build();
        httpClient = HttpAsyncClients.custom()
                .setMaxConnTotal(20)
                .setDefaultRequestConfig(requestConfig).build();
        httpClient.start();
    }

    @Override
    public void close() throws Exception {
        //super.close();
        super.close();
        if (httpClient != null) {
            httpClient.close();
        }
    }

    @Override
    public void asyncInvoke(String line, ResultFuture<String> resultFuture) throws Exception {
        String[] fields = line.split(",");
//        String uid = fields[0];
//        String aid = fields[1];
//        String time = fields[2];
//        int eventType = Integer.parseInt(fields[3]);
        double longitude = Double.parseDouble(fields[4]);
        double latitude = Double.parseDouble(fields[5]);

//        double longitude = 116.311805;
//        double latitude = 40.028572;

        String url = "https://restapi.amap.com/v3/geocode/regeo?key=4924f7ef5c86a278f5500851541cdcff&location=" + longitude +"," + latitude;
        HttpGet httpGet = new HttpGet(url);
        Future<HttpResponse> future = httpClient.execute(httpGet, null);

        CompletableFuture.supplyAsync(new Supplier<String>() {
            @Override
            public String get() {
                try {
                    HttpResponse response = future.get();
                    String res = null;
                    if (response.getStatusLine().getStatusCode() == 200) {
                        res = EntityUtils.toString(response.getEntity());
                    }
                    return res;
                } catch (Exception ex){
                    return null;
                }
            }
        }).thenAccept((String dbResult) -> {
            resultFuture.complete(Collections.singleton(dbResult));
        });
    }
}
