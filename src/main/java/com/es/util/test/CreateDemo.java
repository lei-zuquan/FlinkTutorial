package com.es.util.test;

import com.es.util.ESUtil;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.net.UnknownHostException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;


/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: 2020-03-23 10:40
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */

// 2.结合ESUtil.java ，来做一些测试。首先是创建type为"blog"的Mapping，运行CreateDemo.test()：
public class CreateDemo {

    /**
     * 创建Index
     * @param args
     * @throws UnknownHostException
     */
    public static void main(String[] args) throws Exception {
//        //1.设置集群名称
//        Settings settings = Settings.builder().put("cluster.name", "TestCluster").build();
//        //2.创建client
//        TransportClient client = new PreBuiltTransportClient(settings)
//                .addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9300));
//        //3.获取IndicesAdminClient对象
//        IndicesAdminClient indicesAdminClient = client.admin().indices();
//        //4.创建索引
//        CreateIndexResponse ciReponse=indicesAdminClient.prepareCreate("app_account").get();
//        System.out.println(ciReponse.isAcknowledged());

        boolean status = ESUtil.createIndex("app_account");
        if (status) {
            setMapping();
        }

        System.out.println(status);
    }

    /**
     * 创建Mapping
     */
    private static void setMapping(){
        try {
            XContentBuilder builder = jsonBuilder()
                    .startObject()
                    .startObject("properties")
                    .startObject("id")
                    .field("type", "long")
                    .endObject()
                    .startObject("title")
                    .field("type", "text")
                    .field("analyzer", "ik_max_word")
                    .field("search_analyzer", "ik_max_word")
                    .field("boost", 2)
                    .endObject()
                    .startObject("content")
                    .field("type", "text")
                    .field("analyzer", "ik_max_word")
                    .field("search_analyzer", "ik_max_word")
                    .endObject()
                    .startObject("postdate")
                    .field("type", "date")
                    .field("format", "yyyy-MM-dd HH:mm:ss")
                    .endObject()
                    .startObject("url")
                    .field("type", "keyword")
                    .endObject()
                    .endObject()
                    .endObject();
            System.out.println(builder.string());

            ESUtil.setMapping("app_account", "blog", builder.string());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}