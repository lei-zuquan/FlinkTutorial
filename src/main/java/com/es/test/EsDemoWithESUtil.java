package com.es.test;

import com.es.util.ESUtil;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.filter.Filters;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregator;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Test;

import java.net.InetAddress;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;


public class EsDemoWithESUtil {

    /**
     *     从ES中查询数据
     *     4.1 在Java应用中实现查询文档
     *     GET /lib3/user/1
     */
    @Test
    public void test1() throws Exception {
        // 数据查询
        GetResponse response = ESUtil.selectDocument("lib3", "user", "1");

        // 得到查询出的数据
        System.out.println(response);
    }

    /**
     * 4.2 在Java应用中实现添加文档
     */
    @Test
    public void test2() throws Exception {

        /**
         * 如果需要将上述json数据插入ES，需要先建立mapping，ik_max_word中文分词器
         *   PUT /index1
         *   {
         *     "settings": {
         *         "number_of_shards": 3,
         *         "number_of_replicas": 0
         *     },
         *     "mappings": {
         *       "blog": {
         *         "properties": {
         *           "id":{
         *             "type":"long"
         *           },
         *           "title":{
         *             "type":"text",
         *             "analyzer": "ik_max_word"
         *           },
         *           "content":{
         *             "type": "text",
         *             "analyzer": "ik_max_word"
         *           },
         *           "postdate":{
         *             "type": "date"
         *           },
         *           "url":{
         *             "type": "text"
         *           }
         *         }
         *       }
         *     }
         *   }
         */
        // 方式一
        String json = "{" +
                "\"id\":\"3\"," +
                "\"title\":\"Java设计模式之单例模式\"," +
                "\"content\":\"枚举单例模式可以防反射攻击。\"," +
                "\"postdate\":\"2018-02-03\"," +
                "\"url\":\"csdn.net/79247746\"" +
                "}";
        System.out.println(json);

        ESUtil.insertDocument("index1", "blog", "10", json);

        // 方式二
        XContentBuilder doc = jsonBuilder()
                .startObject()
                .field("id","3")
                .field("title","Java设计模式之单例模式")
                .field("content","枚举单例模式可以防反射攻击。")
                .field("postdate","2018-02-03")
                .field("url","csdn.net/79247746")
                .endObject();
        ESUtil.insertDocument("index1", "blog", "10", doc);

        /**
         * 上述是插入数据，接下来进行数据的查询
         * GET /app_account/blog/_search
         */

        GetResponse response = ESUtil.selectDocument("index1", "blog", "10");
    }


    /**
     * 删除文档
     * DELETE /index1/blog/10
      */
    @Test
    public void test3() throws Exception {

        DeleteResponse response = ESUtil.deleteDocument("index1", "blog", "10");

        //删除成功返回OK，否则返回NOT_FOUND
        System.out.println(response.status());
    }

    /**
     * 更新文档
      */
    @Test
    public void test4() throws Exception {

        XContentBuilder doc = jsonBuilder().startObject()
                .field("title", "单例模式解读")
                .endObject();
        UpdateResponse response = ESUtil.updateDocument("index1", "blog", "10", doc);

        //更新成功返回OK，否则返回NOT_FOUND
        System.out.println(response.status());
    }

    // upsert方式：如果文档不存在则插入，如果存在则修改
    @Test
    public void test5() throws Exception {

        // 添加文档
        XContentBuilder insertDoc = XContentFactory.jsonBuilder().startObject()
                                .field("id", "3")
                                .field("title", "装饰模式")
                                .field("content", "动态地扩展一个对象的功能")
                                .field("postdate", "2018-05-23")
                                .field("url", "csdn.net/79239072")
                                .endObject();
        // upsert操作：如果request2操作文档不存在，则执行request1文档的添加
        XContentBuilder updateDoc = XContentFactory.jsonBuilder().startObject()
                                .field("title", "装饰模式解读")
                                .endObject();

        UpdateResponse response = ESUtil.upsertDocument("index1", "blog", "8", insertDoc, updateDoc);

        //upsert操作成功返回OK，否则返回NOT_FOUND
        System.out.println(response.status());
    }

    /**
     * 4.5 在Java应用中实现批量操作
     * mget指量查询，只能查询
      */
    @Test
    public void test6() throws Exception {
        // 指定ES集群
        // 在配置文件vi /opt/elasticsearch-6.2.4/config/elasticsearch.yml
        // cluster.name: my-application (需要打开)
        // node.name: node-1 (需要打开)
        Settings settings = Settings.builder().put("cluster.name", "my-application").build();
        // 创建访问es服务器的客户端
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("172.19.125.190"), 9300));


        MultiGetResponse mgResponse = client.prepareMultiGet()
                .add("index1", "blog", "8", "10")
                .add("lib3", "user", "1", "2", "3")
                .get();

        for (MultiGetItemResponse response : mgResponse) {
            GetResponse rp = response.getResponse();
            if (rp != null && rp.isExists()) {
                System.out.println(rp.getSourceAsString());
            }
        }
    }

    /**
     * bulk：批量操作。增、删、改、查
     */
    @Test
    public void test7() throws Exception {
        // 指定ES集群
        // 在配置文件vi /opt/elasticsearch-6.2.4/config/elasticsearch.yml
        // cluster.name: my-application (需要打开)
        // node.name: node-1 (需要打开)
        Settings settings = Settings.builder().put("cluster.name", "my-application").build();
        // 创建访问es服务器的客户端
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("172.19.125.190"), 9300));

        BulkRequestBuilder bulkRequest = client.prepareBulk();

        bulkRequest.add(client.prepareIndex("lib2", "books", "8")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("title", "python")
                        .field("price", 99)
                        .endObject()
                )
        );
        bulkRequest.add(client.prepareIndex("lib2", "books", "9")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("title", "VR")
                        .field("price", 29)
                        .endObject()
                )
        );

        //批量执行
        BulkResponse bulkResponse = bulkRequest.get();
        System.out.println(bulkResponse.status());

        if (bulkResponse.hasFailures()) {

            System.out.println("存在失败操作");
        }
    }

    // 查询删除：将查询出来的数据进行删除
    @Test
    public void test8() throws Exception {
        // 指定ES集群
        // 在配置文件vi /opt/elasticsearch-6.2.4/config/elasticsearch.yml
        // cluster.name: my-application (需要打开)
        // node.name: node-1 (需要打开)
        Settings settings = Settings.builder().put("cluster.name", "my-application").build();
        // 创建访问es服务器的客户端
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("172.19.125.190"), 9300));

        BulkByScrollResponse response = DeleteByQueryAction.INSTANCE
                .newRequestBuilder(client)
                // 查询过虑条件
                .filter(QueryBuilders.matchQuery("title", "解读"))
                .source("index1")
                .get();
        long counts = response.getDeleted();
        System.out.println("删除文档个数：" + counts);
    }

    // 4.7 查询所有 matchAll
    @Test
    public void test9() throws Exception {
        // 指定ES集群
        // 在配置文件vi /opt/elasticsearch-6.2.4/config/elasticsearch.yml
        // cluster.name: my-application (需要打开)
        // node.name: node-1 (需要打开)
        Settings settings = Settings.builder().put("cluster.name", "my-application").build();
        // 创建访问es服务器的客户端
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("172.19.125.190"), 9300));

        QueryBuilder qb = QueryBuilders.matchAllQuery();

        SearchResponse sr = client.prepareSearch("lib3")
                .setQuery(qb)
                .setSize(3).get(); // 每次查3个

        SearchHits hits = sr.getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());

            Map<String, Object> map = hit.getSourceAsMap();
            for (String key : map.keySet()) {
                System.out.println(key + "=" + map.get(key));
            }
        }

    }

    // 4.8 macth query 指定条件查询
    @Test
    public void test10() throws Exception {
        // 指定ES集群
        // 在配置文件vi /opt/elasticsearch-6.2.4/config/elasticsearch.yml
        // cluster.name: my-application (需要打开)
        // node.name: node-1 (需要打开)
        Settings settings = Settings.builder().put("cluster.name", "my-application").build();
        // 创建访问es服务器的客户端
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("172.19.125.190"), 9300));

        QueryBuilder builder = QueryBuilders.matchQuery("interests", "changge");

        SearchResponse response = client.prepareSearch("lib3")
                .setQuery(builder)
                .setSize(3)
                .get();
        SearchHits hits = response.getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());

            Map<String, Object> map = hit.getSourceAsMap();
            for (String key : map.keySet()) {
                System.out.println(key + "=" + map.get(key));
            }
        }
    }


    // 4.9 multiMatchQuery 指定多个字段
    @Test
    public void test11() throws Exception {
        // 指定ES集群
        // 在配置文件vi /opt/elasticsearch-6.2.4/config/elasticsearch.yml
        // cluster.name: my-application (需要打开)
        // node.name: node-1 (需要打开)
        Settings settings = Settings.builder().put("cluster.name", "my-application").build();
        // 创建访问es服务器的客户端
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("172.19.125.190"), 9300));

        // 增加一个字段查询
        QueryBuilder qb = QueryBuilders.multiMatchQuery("changge", "address", "interests");
        SearchResponse response = client.prepareSearch("lib3")
                .setQuery(qb)
                .setSize(3)
                .get();
        SearchHits hits = response.getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());

            Map<String, Object> map = hit.getSourceAsMap();
            for (String key : map.keySet()) {
                System.out.println(key + "=" + map.get(key));
            }
        }
    }


    // 4.10 term查询 term：查询某个字段里含有某个关键词的文档
    @Test
    public void test12() throws Exception {
        // 指定ES集群
        // 在配置文件vi /opt/elasticsearch-6.2.4/config/elasticsearch.yml
        // cluster.name: my-application (需要打开)
        // node.name: node-1 (需要打开)
        Settings settings = Settings.builder().put("cluster.name", "my-application").build();
        // 创建访问es服务器的客户端
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("172.19.125.190"), 9300));

        QueryBuilder builder = QueryBuilders.termQuery("interests", "changge");

        SearchResponse response = client.prepareSearch("lib3")
                .setQuery(builder)
                .setSize(2).get();
        SearchHits hits = response.getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }


    //4.11 terms查询 terms：查询某个字段里含有多个关键词的文档
    @Test
    public void test13() throws Exception {
        // 指定ES集群
        // 在配置文件vi /opt/elasticsearch-6.2.4/config/elasticsearch.yml
        // cluster.name: my-application (需要打开)
        // node.name: node-1 (需要打开)
        Settings settings = Settings.builder().put("cluster.name", "my-application").build();
        // 创建访问es服务器的客户端
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("172.19.125.190"), 9300));

        // 较term，可以输入多个词条
        QueryBuilder builder = QueryBuilders.termsQuery("interests", "changge", "lvyou");

        SearchResponse response = client.prepareSearch("lib3")
                .setQuery(builder)
                .setSize(2).get();
        SearchHits hits = response.getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }

    // 各种查询
    @Test
    public void test14() throws Exception {
        // 指定ES集群
        // 在配置文件vi /opt/elasticsearch-6.2.4/config/elasticsearch.yml
        // cluster.name: my-application (需要打开)
        // node.name: node-1 (需要打开)
        Settings settings = Settings.builder().put("cluster.name", "my-application").build();
        // 创建访问es服务器的客户端
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("172.19.125.190"), 9300));

        // range查询：范围查询
        //QueryBuilder builder = QueryBuilders.rangeQuery("birthday").from("1990-01-01").to("2000-12-30").format("yyyy-MM-dd");

        // prifix查询：前缀查询
        //QueryBuilder builder = QueryBuilders.prefixQuery("name", "zhao");

        //wildcard查询，通配符查询
        //QueryBuilder builder = QueryBuilders.wildcardQuery("name", "zhao*");

        // fuzzy查询，模糊查询
        //QueryBuilder builder = QueryBuilders.fuzzyQuery("interests", "chagge");

        // type查询
        QueryBuilder builder = QueryBuilders.typeQuery("blog");

        // id查询
        //QueryBuilder builder = QueryBuilders.idsQuery().addIds("1", "3");


        SearchResponse response = client.prepareSearch("lib3")
                .setQuery(builder)
                //.setSize(2)
                .get();

        SearchHits hits = response.getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }

    // 4.13 聚合查询: 求最大、最小、平均、总和、基数
    @Test
    public void test15() throws Exception {
        // 指定ES集群
        // 在配置文件vi /opt/elasticsearch-6.2.4/config/elasticsearch.yml
        // cluster.name: my-application (需要打开)
        // node.name: node-1 (需要打开)
        Settings settings = Settings.builder().put("cluster.name", "my-application").build();
        // 创建访问es服务器的客户端
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("172.19.125.190"), 9300));

        /* 求最大值
        AggregationBuilder agg = AggregationBuilders.max("aggMax").field("age");
        SearchResponse response = client.prepareSearch("lib3").addAggregation(agg).get();
        Max max = response.getAggregations().get("aggMax");
        System.out.println(max.getValue());
        */

        /* 求最小值
        AggregationBuilder agg = AggregationBuilders.min("aggMin").field("age");
        SearchResponse response = client.prepareSearch("lib3").addAggregation(agg).get();
        Min min = response.getAggregations().get("aggMin");
        System.out.println(min.getValue());
        */

        /* 求平均值
        AggregationBuilder agg = AggregationBuilders.avg("aggAvg").field("age");
        SearchResponse response = client.prepareSearch("lib3").addAggregation(agg).get();
        Avg avg = response.getAggregations().get("aggAvg");
        System.out.println(avg.getValue());
        */

        /* 求总和
        AggregationBuilder agg = AggregationBuilders.sum("aggSum").field("age");
        SearchResponse response = client.prepareSearch("lib3").addAggregation(agg).get();
        Sum sum = response.getAggregations().get("aggSum");
        System.out.println(sum.getValue());
        */

        /* 获取值互不相同的数量 */
        AggregationBuilder agg = AggregationBuilders.cardinality("aggCardinality").field("age");
        SearchResponse response = client.prepareSearch("lib3").addAggregation(agg).get();
        Cardinality cardinality = response.getAggregations().get("aggCardinality");
        System.out.println(cardinality.getValue());
    }


    // query string 相当于：GET /myindex/article/_search?q=content:html
    @Test
    public void tests() throws Exception {
        // 指定ES集群
        // 在配置文件vi /opt/elasticsearch-6.2.4/config/elasticsearch.yml
        // cluster.name: my-application (需要打开)
        // node.name: node-1 (需要打开)
        Settings settings = Settings.builder().put("cluster.name", "my-application").build();
        // 创建访问es服务器的客户端
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("172.19.125.190"), 9300));

        // 指定字段，精确查找
        // QueryBuilder builder = QueryBuilders.commonTermsQuery("name", "zhaoliu");

        // 不需要指定字段，全文搜索。有唱歌，没有喝酒。但是需要满足所有条件
        //QueryBuilder builder = QueryBuilders.queryStringQuery("+changge -hejiu");

        // 不需要指定字段，全文搜索。有唱歌，没有喝酒。只需要满足其中一个条件即可
        QueryBuilder builder = QueryBuilders.simpleQueryStringQuery("+changge -hejiu");

        SearchResponse response = client.prepareSearch("lib3")
                .setQuery(builder)
                //.setSize(2)
                .get();

        SearchHits hits = response.getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }


    // 组合查询
    @Test
    public void tests16() throws Exception {
        // 指定ES集群
        // 在配置文件vi /opt/elasticsearch-6.2.4/config/elasticsearch.yml
        // cluster.name: my-application (需要打开)
        // node.name: node-1 (需要打开)
        Settings settings = Settings.builder().put("cluster.name", "my-application").build();
        // 创建访问es服务器的客户端
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("172.19.125.190"), 9300));

        // bool实现复合查询；必须满足；必须不满足；可以满足；过滤
//      QueryBuilder builder = QueryBuilders.boolQuery()
//                                .must(QueryBuilders.matchQuery("interests", "changge"))
//                                .mustNot(QueryBuilders.matchQuery("interests", "lvyou"))
//                                .should(QueryBuilders.matchQuery("address", "bei jing"))
//                                .filter(QueryBuilders.rangeQuery("birthday").gte("1990-01-01").format("yyyy-MM-dd"));

        // constantscore：不计算相关度分数
        QueryBuilder builder = QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("name", "zhaoliu"));

        SearchResponse response = client.prepareSearch("lib3")
                .setQuery(builder)
                //.setSize(2)
                .get();

        SearchHits hits = response.getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }

    // 分组聚合
    @Test
    public void teststerms() throws Exception {
        // 指定ES集群
        // 在配置文件vi /opt/elasticsearch-6.2.4/config/elasticsearch.yml
        // cluster.name: my-application (需要打开)
        // node.name: node-1 (需要打开)
        Settings settings = Settings.builder().put("cluster.name", "my-application").build();
        // 创建访问es服务器的客户端
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("172.19.125.190"), 9300));

        // 分组
        AggregationBuilder agg = AggregationBuilders.terms("terms").field("age");

        SearchResponse response = client.prepareSearch("lib3").addAggregation(agg).get();

        Terms terms = response.getAggregations().get("terms");
        for ( Terms.Bucket entry: terms.getBuckets() ) {
            System.out.println(entry.getKey() + ":" + entry.getDocCount());
        }

    }

    // filter聚合; 只统计年龄满足20岁的个数
    @Test
    public void testfilter() throws Exception {
        // 指定ES集群
        // 在配置文件vi /opt/elasticsearch-6.2.4/config/elasticsearch.yml
        // cluster.name: my-application (需要打开)
        // node.name: node-1 (需要打开)
        Settings settings = Settings.builder().put("cluster.name", "my-application").build();
        // 创建访问es服务器的客户端
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("172.19.125.190"), 9300));

        QueryBuilder query = QueryBuilders.termQuery("age", 20);
        AggregationBuilder agg = AggregationBuilders.filter("filter", query);

        SearchResponse response = client.prepareSearch("lib3").addAggregation(agg).get();

        Filter filter = response.getAggregations().get("filter");
        System.out.println(filter.getDocCount());

    }
    // filters聚合; 统计唱歌、喝酒个数
    @Test
    public void testfilters() throws Exception {
        // 指定ES集群
        // 在配置文件vi /opt/elasticsearch-6.2.4/config/elasticsearch.yml
        // cluster.name: my-application (需要打开)
        // node.name: node-1 (需要打开)
        Settings settings = Settings.builder().put("cluster.name", "my-application").build();
        // 创建访问es服务器的客户端
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("172.19.125.190"), 9300));

        QueryBuilder query = QueryBuilders.termQuery("age", 20);
        AggregationBuilder agg = AggregationBuilders.filters("filters",
                new FiltersAggregator.KeyedFilter("changge", QueryBuilders.termQuery("interests", "changge")),
                new FiltersAggregator.KeyedFilter("hejiu", QueryBuilders.termQuery("interests", "hejiu")));

        SearchResponse response = client.prepareSearch("lib3").addAggregation(agg).execute().actionGet();

        Filters filters = response.getAggregations().get("filters");
        for (Filters.Bucket entry : filters.getBuckets( )) {
            System.out.println(entry.getKey() + ":" + entry.getDocCount());
        }

    }

    // range聚合分组统计
    @Test
    public void testrange() throws Exception {
        // 指定ES集群
        // 在配置文件vi /opt/elasticsearch-6.2.4/config/elasticsearch.yml
        // cluster.name: my-application (需要打开)
        // node.name: node-1 (需要打开)
        Settings settings = Settings.builder().put("cluster.name", "my-application").build();
        // 创建访问es服务器的客户端
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("172.19.125.190"), 9300));


        AggregationBuilder agg = AggregationBuilders
                .range("range")
                .field("age")
                .addUnboundedTo(50) // (, to) 小于50
                .addRange(25, 50) //[from, to) 25到50，不含50
                .addUnboundedFrom(25); //[from,) 25及25以上

        SearchResponse response = client.prepareSearch("lib3").addAggregation(agg).execute().actionGet();

        Range r = response.getAggregations().get("range");
        for (Range.Bucket entry : r.getBuckets( )) {
            System.out.println(entry.getKey() + ":" + entry.getDocCount());
        }

    }

    // missing聚合; 统计价格为null的文档个数
    @Test
    public void testmissing() throws Exception {
        // 指定ES集群
        // 在配置文件vi /opt/elasticsearch-6.2.4/config/elasticsearch.yml
        // cluster.name: my-application (需要打开)
        // node.name: node-1 (需要打开)
        Settings settings = Settings.builder().put("cluster.name", "my-application").build();
        // 创建访问es服务器的客户端
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("172.19.125.190"), 9300));

        AggregationBuilder agg = AggregationBuilders.missing("missing").field("price");

        SearchResponse response = client.prepareSearch("lib4").addAggregation(agg).execute().get();
        Aggregation aggregation = response.getAggregations().get("missing");
        System.out.println(aggregation.toString());
    }


}
