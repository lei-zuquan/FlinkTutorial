package com.lei.util;

import com.lei.domain.J_SensorReading;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: 2020-05-25 15:47
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
public class J_MyEsUtil {

    private static List<HttpHost> httpHosts = new ArrayList<HttpHost>();

    static {
        httpHosts.add(new HttpHost("elasticsearch01",9200,"http"));
        httpHosts.add(new HttpHost("elasticsearch02",9200,"http"));
        httpHosts.add(new HttpHost("elasticsearch03",9200,"http"));
    }

    public static ElasticsearchSink<J_SensorReading> getElasticSearchSink(String indexName) {
        ElasticsearchSinkFunction<J_SensorReading> esFunc = new ElasticsearchSinkFunction<J_SensorReading>() {

            @Override
            public void process(J_SensorReading element, RuntimeContext ctx, RequestIndexer indexer) {
                System.out.println("saving data：" + element);
                // 包装成一个Map或者JsonObject
                HashMap json = new HashMap<String, String>();
                json.put("sensor_id", element.id);
                json.put("temperature", element.temperature.toString());
                json.put("ts", element.timestamp.toString());

                // 创建index request, 准备发送数据
                IndexRequest indexRequest = Requests.indexRequest()
                        .index(indexName)
                        .type("_doc")
                        .source(json);

                // 利用index发送请求，写入数据
                indexer.add(indexRequest);
                System.out.println("data saved...");
            }
        };

        ElasticsearchSink.Builder<J_SensorReading> sinkBuilder = new ElasticsearchSink.Builder<>(httpHosts, esFunc);

        //刷新前缓冲的最大动作量
        sinkBuilder.setBulkFlushMaxActions(10);


        return sinkBuilder.build();
    }

}