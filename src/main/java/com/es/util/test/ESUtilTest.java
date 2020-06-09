package com.es.util.test;

import com.es.util.ESUtil;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.mustache.SearchTemplateRequestBuilder;
import org.elasticsearch.search.SearchHit;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: 2020-04-02 14:31
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
public class ESUtilTest {
    
    @Test
    public void testMultiGet() throws Exception{
        MultiGetResponse multiGetItemResponses = ESUtil.selectMultiGet(
                "falvku",
                "word",
                "1",
                "2",
                "3");
        for (MultiGetItemResponse multiGetItemResponse : multiGetItemResponses) {
            GetResponse getResponse = multiGetItemResponse.getResponse();
            if (getResponse.isExists()){
                System.out.println(getResponse.getSourceAsString());
            }
        }
    }
    
    @Test
    public void testBulkTest() throws Exception {
        BulkRequestBuilder bulkRequestBuilder = ESUtil.getBulkRequestBuilder();

        IndexRequestBuilder indexRequestBuilder = ESUtil.getClient().prepareIndex("falvku", "word", "3")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("title", "插入主题")
                        .field("name", "插入主题正文")
                        .endObject());
        bulkRequestBuilder.add(indexRequestBuilder);

        UpdateRequestBuilder updateRequestBuilder = ESUtil.getClient().prepareUpdate("falvku", "word", "3")
                .setDoc(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("title", "插入主题更新")
                        .endObject());
        bulkRequestBuilder.add(updateRequestBuilder);

        DeleteRequestBuilder deleteReqeustBuilder = ESUtil.getClient().prepareDelete("falvku", "word", "2");
        bulkRequestBuilder.add(deleteReqeustBuilder);

        BulkResponse bulkResponse = bulkRequestBuilder.get();

        for(BulkItemResponse bulkItemResponse : bulkResponse.getItems()) {
            System.out.println("version: " + bulkItemResponse.getVersion());
        }
    }

    /**
     * 分批查询，提升查询性能
     */
    @Test
    public void testScroll(){
        SearchResponse searchResponse = ESUtil.getClient().prepareSearch("falvku")
                .setTypes("word")
                .setQuery(QueryBuilders.termQuery("title", "中国"))
                .setScroll(new TimeValue(60000))
                .setSize(100)
                .get();

        int batchCount = 0;

        do {
            System.out.println("batch: " + ++batchCount);
            for(SearchHit searchHit : searchResponse.getHits().getHits()) {

                System.out.println(searchHit.getSourceAsString());

                // 每次查询一批数据，比如1000行，然后写入本地的一个excel文件中

                // 如果说你一下子查询几十万条数据，不现实，jvm内存可能都会爆掉
            }

            searchResponse = ESUtil.getClient().prepareSearchScroll(searchResponse.getScrollId())
                    .setScroll(new TimeValue(60000))
                    .execute()
                    .actionGet();
        } while(searchResponse.getHits().getHits().length != 0);

        ESUtil.getClient().close();

    }

    /**
     * 模版查询，暂时没有调试成功
     */
    @Test
    public void testSearchTemplate(){
        Map<String, Object> scriptParams = new HashMap<>();
        scriptParams.put("from", 0);
        scriptParams.put("size", 1);
        scriptParams.put("title", "中国");

        SearchResponse searchResponse = new SearchTemplateRequestBuilder(ESUtil.getClient())
                .setScript("page_query_by_brand")
                .setScriptType(ScriptType.INLINE)
                //.setScriptType(ScriptType.INLINE)
                .setScriptParams(scriptParams)
                .setRequest(new SearchRequest("falvku").types("word"))
                .get()
                .getResponse();

        for(SearchHit searchHit : searchResponse.getHits().getHits()) {
            System.out.println(searchHit.getSourceAsString());
        }

        ESUtil.getClient().close();
    }

    @Test
    public void testFullTextSelect(){
        SearchResponse searchResponse = ESUtil.getClient().prepareSearch("falvku")
                .setTypes("word")
                .setQuery(QueryBuilders.matchQuery("title", "新股上市"))
                .get();

        for(SearchHit searchHit : searchResponse.getHits().getHits()) {
            System.out.println(searchHit.getSourceAsString());
        }

        System.out.println("====================================================");

        searchResponse = ESUtil.getClient().prepareSearch("falvku")
                .setTypes("word")
                .setQuery(QueryBuilders.multiMatchQuery("宝", "title", "content"))
                .get();

        for(SearchHit searchHit : searchResponse.getHits().getHits()) {
            System.out.println(searchHit.getSourceAsString());
        }

        System.out.println("====================================================");

        searchResponse = ESUtil.getClient().prepareSearch("falvku")
                .setTypes("word")
                .setQuery(QueryBuilders.termQuery("title", "国务院"))
                .get();

        for(SearchHit searchHit : searchResponse.getHits().getHits()) {
            System.out.println(searchHit.getSourceAsString());
        }

        System.out.println("====================================================");

        searchResponse = ESUtil.getClient().prepareSearch("falvku")
                .setTypes("word")
                .setQuery(QueryBuilders.prefixQuery("title", "证监会"))
                .get();

        for(SearchHit searchHit : searchResponse.getHits().getHits()) {
            System.out.println(searchHit.getSourceAsString());
        }

        ESUtil.getClient().close();
    }


    @Test
    public void testBoolQuery(){
        QueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .must(QueryBuilders.matchQuery("title", "新股上市"))
                .mustNot(QueryBuilders.termQuery("title.raw", "新股上市318"));
                //.should(QueryBuilders.rangeQuery("produce_date").gte("2017-01-01").lte("2017-01-31"))
                //.filter(QueryBuilders.rangeQuery("price").gte(280000).lte(350000));

        SearchResponse searchResponse = ESUtil.getClient().prepareSearch("falvku")
                .setTypes("word")
                .setQuery(queryBuilder)
                .get();

        for(SearchHit searchHit : searchResponse.getHits().getHits()) {
            System.out.println(searchHit.getSourceAsString());
        }

        ESUtil.getClient().close();
    }

    @Test
    public void testPos(){
        /*
        DELETE /car_shop
PUT /car_shop
{
    "mappings": {
        "cars": {
            "properties": {
                "brand": {
                    "type": "text",
                    "analyzer": "ik_max_word",
                    "fields": {
                        "raw": {
                            "type": "keyword"
                        }
                    }
                },
                "name": {
                    "type": "text",
                    "analyzer": "ik_max_word",
                    "fields": {
                        "raw": {
                            "type": "keyword"
                        }
                    }
                },
                 "location": {
                  "type": "geo_point"
                }
            }
        }
    }
}


PUT /car_shop/cars/1
{
    "name": "上海至全宝马4S店",
    "pin" : {
        "location" : {
            "lat" : 40.12,
            "lon" : -71.34
        }
    }
}

         */

        SearchResponse searchResponse = ESUtil.getClient().prepareSearch("car_shop")
                .setTypes("cars")
                .setQuery(QueryBuilders.geoBoundingBoxQuery("location")
                        .setCorners(40.73, -74.1, 40.01, -71.12))
                .get();

        for(SearchHit searchHit : searchResponse.getHits().getHits()) {
            System.out.println(searchHit.getSourceAsString());
        }

        System.out.println("====================================================");

        List<GeoPoint> points = new ArrayList<>();
        points.add(new GeoPoint(40.73, -74.1));
        points.add(new GeoPoint(40.01, -71.12));
        points.add(new GeoPoint(50.56, -90.58));

        searchResponse = ESUtil.getClient().prepareSearch("car_shop")
                .setTypes("cars")
                .setQuery(QueryBuilders.geoPolygonQuery("location", points))
                .get();

        for(SearchHit searchHit : searchResponse.getHits().getHits()) {
            System.out.println(searchHit.getSourceAsString());
        }

        System.out.println("====================================================");

        searchResponse = ESUtil.getClient().prepareSearch("car_shop")
                .setTypes("cars")
                .setQuery(QueryBuilders.geoDistanceQuery("location")
                        .point(40, -70)
                        .distance(200000, DistanceUnit.KILOMETERS))
                .get();

        for(SearchHit searchHit : searchResponse.getHits().getHits()) {
            System.out.println(searchHit.getSourceAsString());
        }

        ESUtil.getClient().close();
    }
}
