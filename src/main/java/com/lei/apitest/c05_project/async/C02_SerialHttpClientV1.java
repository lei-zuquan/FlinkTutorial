package com.lei.apitest.c05_project.async;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
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
        // 构造请求
        // String url = "http://www.baidu.com/";
        String url = "https://www.cnblogs.com/";
        // String url = "https://study.163.com/";
        HttpPost httpPost = new HttpPost(url);
        // httpPost.addHeader("Connection", "keep-alive");

        httpPost.setEntity(null);

        // 异步请求
        long start = System.currentTimeMillis();
        CloseableHttpClient httpClient = HttpClients.createDefault();

        for (int i = 0; i < 90; i++) {
            CloseableHttpResponse httpResponse = httpClient.execute(httpPost);
            try {
                if (httpResponse.getStatusLine().getStatusCode() == 200) {
                    System.out.println("ok: " + i);
                    /*HttpEntity revEntity = httpResponse.getEntity();
                    String res = EntityUtils.toString(revEntity);
                    System.out.println("cost is:"+(System.currentTimeMillis()-start)+":"+ res + " finishedCnt:" + i);*/
                    // System.out.println(httpResponse.getEntity().getContent().toString());
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
