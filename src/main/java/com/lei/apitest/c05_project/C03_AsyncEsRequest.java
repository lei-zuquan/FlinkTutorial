package com.lei.apitest.c05_project;

import com.es.constant.ConfigConstant;
import com.es.util.ESUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

/**
 * @Author:
 * @Date: 2020-06-09 12:43
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */

/*
# flink 操作es 数据
DELETE /flink_es_fruit
PUT /flink_es_fruit
{
  "settings": {
      "number_of_shards": 3,
      "number_of_replicas": 0
  },
  "mappings": {
    "fruit": {
      "properties": {
        "chinese_name":{
          "type":"keyword"
        },
        "english_name":{
          "type":"text",
          "analyzer": "ik_max_word"
        },
        "place":{
          "type": "text",
          "analyzer": "ik_smart"
        }
      }
    }
  }
}

PUT /flink_es_fruit/fruit/1
{
  "chinese_name": "苹果",
  "english_name": "apple",
  "place":"中国山东省"
}
PUT /flink_es_fruit/fruit/2
{
  "chinese_name": "梨",
  "english_name": "pear",
  "place":"中国山东省"
}
PUT /flink_es_fruit/fruit/3
{
  "chinese_name": "香蕉",
  "english_name": "banana",
  "place":"中国海南省"
}
PUT /flink_es_fruit/fruit/4
{
  "chinese_name": "葡萄",
  "english_name": "banana",
  "place":"中国新疆自治区"
}
PUT /flink_es_fruit/fruit/5
{
  "chinese_name": "西瓜",
  "english_name": "watermelon",
  "place":"中国广东省"
}

GET /flink_es_fruit/_search

GET /flink_es_fruit/fruit/_search
{
    "query":{
        "terms":{
            "place": ["山东省"]
        }
    }
}
 */
// 通过 id 查询ES 对应的文档，id 则来自于kafka
public class C03_AsyncEsRequest extends RichAsyncFunction<String, Tuple2<String, String>> {

    private static transient TransportClient client;

    @Override
    public void open(Configuration parameters) throws Exception {
        //super.open(parameters);
        // 设置集群名称
        // 方式一：直接在open方法里编写建立连接，这种不够灵活，后续比如很多地方需要与ES打交道，一一编写
        /*Settings settings = Settings.builder().put("cluster.name", "cluster-elasticsearch-prod").build();
        // 创建client
        client = new PreBuiltTransportClient(settings).addTransportAddresses(
                new TransportAddress(InetAddress.getByName("elasticsearch01"), 9300),
                new TransportAddress(InetAddress.getByName("elasticsearch01"), 9300),
                new TransportAddress(InetAddress.getByName("elasticsearch01"), 9300)
        );*/
        // 方法二：通过工具类获取ES的连接，更加友好，体现设计模式的高内聚低耦合
        client = ESUtil.getClient();
    }

    @Override
    public void close() throws Exception {
        //super.close();
        if (client != null) {
            client.close();
        }
    }

    @Override
    public void asyncInvoke(String key, ResultFuture<Tuple2<String, String>> resultFuture) throws Exception {
        ActionFuture<GetResponse> actionFuture = client.get(new GetRequest("flink_es_fruit", "fruit", key));

        CompletableFuture.supplyAsync(new Supplier<String>() {
            @Override
            public String get() {
                try {
                    GetResponse response = actionFuture.get();
                    return response.getSource().values().toString(); // 返回整条匹配的信息
                    // return response.getSource().get("chinese_name").toString(); // 查询指定字段信息
                } catch (InterruptedException | ExecutionException e) {
                    return null;
                }
            }
        }).thenAccept( (String dbResult) -> {
            resultFuture.complete(Collections.singleton(new Tuple2<>(key, dbResult)));
        });
    }
}
