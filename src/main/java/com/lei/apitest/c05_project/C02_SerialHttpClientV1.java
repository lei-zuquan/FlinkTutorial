package com.lei.apitest.c05_project;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.nio.reactor.ConnectingIOReactor;
import org.apache.http.nio.reactor.IOReactorException;
import org.apache.http.util.EntityUtils;


/**
 * @Author:
 * @Date: 2020-09-15 15:51
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
public class C02_SerialHttpClientV1 {

    public static void main(String[] args) throws Exception {
        //构造请求
        String url = "http://172.24.2.82:8866/predict/ernie_finetuned";
        HttpPost httpPost = new HttpPost(url);
        httpPost.addHeader("Connection", "keep-alive");

        StringEntity entity = null;
        try {
            String requestContent = "投资者提问：从国际上看，实行铁路站场和毗邻区域的综合开发，能够从站场周边物... 投资者提问：从国际上看，实行铁路站场和毗邻区域的综合开发，能够从站场周边物业收益弥补铁路建设资金的不足，已有成功经验可循。贵公司在铁路综合开发和盘活现有铁路用地方面有哪些措施？董秘回答(京沪高铁SH601816)： 您好，京沪高铁建设初期坚持节约用地、集约用地，现有用地均为铁路运输生产用地，公司在抓好主营运输业务的基础上，正在积极探索新形势下多元化经营发展途径，充分挖掘和开发利用公司资产资源潜力，大力培育公司新的利润增长点。感谢您对公司的关注！免责声明：本信息由新浪财经从公开信息中摘录，不构成任何投资建议；新浪财经不保证数据的准确性，内容仅供参考。";
            String jsonParam = "{\"data\":[[\"" + requestContent + "\"]]}";
            entity = new StringEntity(jsonParam, "UTF-8");//解决中文乱码问题
            entity.setContentEncoding("UTF-8");
            entity.setContentType("application/json");

        } catch (Exception e) {
            e.printStackTrace();
        }
        httpPost.setEntity(entity);

        //异步请求

        long start = System.currentTimeMillis();
        CloseableHttpClient httpClient = HttpClients.createDefault();

        for (int i = 0; i < 100; i++) {
            CloseableHttpResponse httpResponse = httpClient.execute(httpPost);
            try {
                if (httpResponse.getStatusLine().getStatusCode() == 200) {
                    HttpEntity revEntity = httpResponse.getEntity();
                    String res = EntityUtils.toString(revEntity);
                    System.out.println("cost is:"+(System.currentTimeMillis()-start)+":"+ res + " finishedCnt:" + i);
                    //System.out.println(httpResponse.getEntity().getContent().toString());
                }
            } finally {
                httpResponse.close();
            }
        }

        long end = System.currentTimeMillis();
        long spend = end - start;
        System.out.println("spend:" + spend);
        httpClient.close();

    }
}
