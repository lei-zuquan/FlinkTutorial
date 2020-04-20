package com.lei.util

import java.util

import com.lei.apitest.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

//import scala.util.parsing.json.JSONObject


/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 11:57 上午 2020/4/18
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */

object MyEsUtil {


  val httpHosts = new util.ArrayList[HttpHost]
  httpHosts.add(new HttpHost("elasticsearch01",9200,"http"))
  httpHosts.add(new HttpHost("elasticsearch02",9200,"http"))
  httpHosts.add(new HttpHost("elasticsearch03",9200,"http"))


  def  getElasticSearchSink(indexName: String): ElasticsearchSink[SensorReading]  ={
    val esFunc = new ElasticsearchSinkFunction[SensorReading] {
      override def process(element: SensorReading, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
        println("saving data：" + element)
        // 包装成一个Map或者JsonObject
        val json = new util.HashMap[String, String]()
        json.put("sensor_id", element.id)
        json.put("temperature", element.temperature.toString)
        json.put("ts", element.timestamp.toString)

        // 创建index request, 准备发送数据
        val indexRequest: IndexRequest = Requests.indexRequest()
          .index(indexName)
          .`type`("_doc")
          .source(json)

        // 利用index发送请求，写入数据
        indexer.add(indexRequest)
        println("data saved...")
      }
    }

    val sinkBuilder = new ElasticsearchSink.Builder[SensorReading](httpHosts, esFunc)

    //刷新前缓冲的最大动作量
    sinkBuilder.setBulkFlushMaxActions(10)


    sinkBuilder.build()
  }

}
