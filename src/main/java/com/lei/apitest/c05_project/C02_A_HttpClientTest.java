package com.lei.apitest.c05_project;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

/**
 * @Author:
 * @Date: 2020-06-09 11:29
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */

// 需求：根据经纬度查询高德API关联位置信息，就是一个普通的 Java程序
public class C02_A_HttpClientTest {
    public static void main(String[] args) throws Exception {
        double longitude = 116.311805;
        double latitude = 40.028572;

        String url = "https://restapi.amap.com/v3/geocode/regeo?key=4924f7ef5c86a278f5500851541cdcff&location=" + longitude +"," + latitude;
        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpGet httpGet = new HttpGet(url);
        CloseableHttpResponse response = httpClient.execute(httpGet);

        try {
            int status = response.getStatusLine().getStatusCode();
            String province = null;
            if (status == 200) {
                // 获取请求的json字符串
                String result = EntityUtils.toString(response.getEntity());
                // 转成json对象
                JSONObject josnObj = JSON.parseObject(result);
                // 获取位置信息
                JSONObject regeocode = josnObj.getJSONObject("regeocode");
                if (regeocode != null && !regeocode.isEmpty()) {
                    JSONObject address = regeocode.getJSONObject("addressComponent");
                    // 获取省市
                    province = address.getString("province");
                    String city = address.getString("city");
                    String businessAreas = address.getString("businessAreas");
                }
            }

            System.out.println(province);
        } finally {
            response.close();
            httpClient.close();
        }
    }
}
