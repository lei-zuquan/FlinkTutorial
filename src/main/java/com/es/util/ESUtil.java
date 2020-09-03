package com.es.util;

import com.es.constant.ConfigConstant;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.lucene.search.function.FieldValueFactorFunction;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FieldValueFactorFunctionBuilder;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.ExecutionException;


/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: 2020-03-23 10:33
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */

/**
 * ES工具类
 */
public class ESUtil {

    // TransportClient对象，用于连接ES集群
    private static volatile TransportClient client;

    private static class TransportClient_Holder {
        private final static TransportClient INSTANCE = getInstance();

        private static TransportClient getInstance() {

            TransportClient tClient = null;
            try {

                //解决netty冲突
                System.setProperty("es.set.netty.runtime.available.processors", "false");
                // 指定ES集群
                // 在配置文件vi /opt/elasticsearch-6.2.4/config/elasticsearch.yml
                // cluster.name: my-application (需要打开)
                // node.name: node-1 (需要打开)
                //Settings settings = Settings.builder().put("cluster.name", "my-application").build();
                // 创建访问es服务器的客户端
                //TransportClient client = new PreBuiltTransportClient(settings)
                //        .addTransportAddress(new TransportAddress(InetAddress.getByName("172.19.125.190"), 9300));

                //构建Settings对象
                Class.forName("com.es.constant.ConfigConstant");
                        /*
                        Settings settings = Settings.builder().put("cluster.name", ConfigConstant.ES_CLUSTER_NAME).build();

                        client = new PreBuiltTransportClient(settings)
                                // 实际生产中不会这个采用，因为有1000个节点那就得写1000个
                                //.addTransportAddress(new TransportAddress(InetAddress.getByName(ConfigConstant.ES_HOST_NAME1), ConfigConstant.ES_TCP_PORT))
                                //.addTransportAddress(new TransportAddress(InetAddress.getByName(ConfigConstant.ES_HOST_NAME2), ConfigConstant.ES_TCP_PORT))
                                .addTransportAddress(new TransportAddress(InetAddress.getByName(ConfigConstant.ES_HOST_NAME), ConfigConstant.ES_TCP_PORT));
                                */
                Settings settings = Settings.builder()
                        .put("cluster.name", ConfigConstant.ES_CLUSTER_NAME)
                        .put("client.transport.sniff", true)
                        .build();
                tClient = new PreBuiltTransportClient(settings);

                String esIps[] = ConfigConstant.ES_HOST_NAME.split(",");
                for (String esIp : esIps) {//添加集群IP列表
                    TransportAddress transportAddress = new TransportAddress(InetAddress.getByName(esIp), ConfigConstant.ES_TCP_PORT);
                    tClient.addTransportAddresses(transportAddress);
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
            return tClient;
        }
    }

    public static TransportClient getClient() {
        return TransportClient_Holder.getInstance();
    }

    /**
     * 获取索引管理的IndicesAdminClient
     */
    public static IndicesAdminClient getAdminClient() {
        return getClient().admin().indices();
    }

    /**
     * 判定索引是否存在
     *
     * @param indexName
     * @return
     */
    public static boolean isExists(String indexName) {
        IndicesExistsResponse response = getAdminClient().prepareExists(indexName).get();
        return response.isExists() ? true : false;
    }

    /**
     * 创建索引
     *
     * @param indexName
     * @return
     */
    public static boolean createIndex(String indexName) {
        CreateIndexResponse createIndexResponse = getAdminClient()
                .prepareCreate(indexName.toLowerCase())
                .get();
        return createIndexResponse.isAcknowledged() ? true : false;
    }

    /**
     * 创建索引
     *
     * @param indexName 索引名
     * @param shards    分片数
     * @param replicas  副本数
     * @return
     */
    public static boolean createIndex(String indexName, int shards, int replicas) {
        Settings settings = Settings.builder()
                .put("index.number_of_shards", shards)
                .put("index.number_of_replicas", replicas)
                .build();
        CreateIndexResponse createIndexResponse = getAdminClient()
                .prepareCreate(indexName.toLowerCase())
                .setSettings(settings)
                .execute().actionGet();
        return createIndexResponse.isAcknowledged() ? true : false;
    }

    /**
     * 为索引indexName设置mapping
     *
     * @param indexName
     * @param typeName
     * @param mapping
     */
    public static void setMapping(String indexName, String typeName, String mapping) {
        getAdminClient().preparePutMapping(indexName)
                .setType(typeName)
                .setSource(mapping, XContentType.JSON)
                .get();
    }

    /**
     * 删除索引
     *
     * @param indexName
     * @return
     */
    public static boolean deleteIndex(String indexName) {
        DeleteIndexResponse deleteResponse = getAdminClient()
                .prepareDelete(indexName.toLowerCase())
                .execute()
                .actionGet();
        return deleteResponse.isAcknowledged() ? true : false;
    }

    /**
     * 插入文档
     *
     * @param indexName 索引名
     * @param type      类型
     * @param doc       XContentBuilder
     */
    public static IndexResponse insertDocument(String indexName, String type, XContentBuilder doc) {
        IndexResponse response = getClient().prepareIndex(indexName, type)
                .setSource(doc)
                .get();
        //System.out.println(response.status());
        return response;
    }

    /**
     * 插入文档
     *
     * @param indexName 索引名
     * @param type      类型
     * @param id        文档id
     * @param doc       XContentBuilder
     */
    public static void insertDocument(String indexName, String type, String id, XContentBuilder doc) {
        IndexResponse response = getClient().prepareIndex(indexName, type, id)
                .setSource(doc)
                .get();
        System.out.println(response.status());
    }

    /**
     * 插入文档
     *
     * @param indexName 索引名
     * @param type      类型
     * @param json      Json格式串
     */
    public static IndexResponse insertDocument(String indexName, String type, String json) {
        IndexResponse response = getClient().prepareIndex(indexName, type)
                .setSource(json, XContentType.JSON)
                .get();
        // System.out.println(response.status());
        return response;
    }

    /**
     * 插入文档
     *
     * @param indexName 索引名
     * @param type      类型
     * @param id        文档id
     * @param json      Json格式串
     */
    public static IndexResponse insertDocument(String indexName, String type, String id, String json) {
        IndexResponse response = getClient().prepareIndex(indexName, type, id)
                .setSource(json, XContentType.JSON)
                .get();
        //System.out.println(response.status());
        return response;
    }

    /**
     * 查询文档
     *
     * @param indexName 索引名
     * @param type      类型
     * @param id        文档id
     * @return
     */
    public static GetResponse selectDocument(String indexName, String type, String id) {
        GetResponse response = getClient().prepareGet(indexName, type, id).get();
        //System.out.println(response.isExists());
        //System.out.println(response.getIndex());
        //System.out.println(response.getType());
        //System.out.println(response.getId());
        //System.out.println(response.getVersion());
        return response;
    }

    /**
     * 查询文档，返回指定字段数据
     *
     * @param indexName 索引名
     * @param type      类型
     * @param id        文档id
     * @return
     */
    public static GetResponse selectDocument(String indexName, String type, String id, String field) {
        GetResponse response = getClient().prepareGet(indexName, type, id)
                .setFetchSource(new String[]{field}, null) // 查询指定字段
                .get();
        //System.out.println(response.isExists());
        //System.out.println(response.getIndex());
        //System.out.println(response.getType());
        //System.out.println(response.getId());
        //System.out.println(response.getVersion());
        return response;
    }

    /**
     * 删除文档
     *
     * @param indexName 索引名
     * @param type      类型
     * @param id        文档id
     */
    public static DeleteResponse deleteDocument(String indexName, String type, String id) {
        DeleteResponse response = getClient().prepareDelete(indexName, type, id).get();
        //删除成功返回OK，否则返回NOT_FOUND
        //System.out.println(response.status());
        //返回被删除文档的类型
        //System.out.println(response.getType());
        //返回被删除文档的ID
        //System.out.println(response.getId());
        //返回被删除文档的版本信息
        //System.out.println(response.getVersion());

        return response;
    }

    /**
     * 修改文档
     *
     * @param indexName 索引名
     * @param type      类型
     * @param id        文档id
     * @param doc       Json
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static UpdateResponse updateDocument(String indexName, String type, String id, XContentBuilder doc) throws Exception {
        UpdateRequest request = new UpdateRequest();
        request.index(indexName).type(type).id(id).doc(doc);
        UpdateResponse response = getClient().update(request).get();
        //更新成功返回OK，否则返回NOT_FOUND
        //System.out.println(response.status());
        //返回被更新文档的类型
        //System.out.println(response.getType());
        //返回被更新文档的ID
        //System.out.println(response.getId());
        //返回被更新文档的版本信息
        //System.out.println(response.getVersion());

        return response;
    }

    /**
     * 更新数据，存在文档则使用updateDoc，不存在则使用insertDoc
     *
     * @param indexName 索引名
     * @param type      类型
     * @param id        文档id
     * @param insertDoc 新文档
     * @param updateDoc 更新文档
     */
    public static UpdateResponse upsertDocument(String indexName, String type, String id, XContentBuilder insertDoc, XContentBuilder updateDoc) throws ExecutionException, InterruptedException {
        IndexRequest indexRequest = new IndexRequest(indexName, type, id)
                .source(insertDoc);
        UpdateRequest updateRequest = new UpdateRequest(indexName, type, id)
                .doc(updateDoc).upsert(indexRequest);
        UpdateResponse response = getClient().update(updateRequest).get();
        //upsert操作成功返回OK，否则返回NOT_FOUND
        //System.out.println(response.status());
        //返回被操作文档的类型
        //System.out.println(response.getType());
        //返回被操作文档的ID
        //System.out.println(response.getId());
        //返回被操作文档的版本信息
        //System.out.println(response.getVersion());

        return response;
    }


    /**
     * 查询所有 matchAll
     * @param indexName
     * @return
     */
    public static SearchHits getMatchAll(String indexName, String fieldName){
        QueryBuilder qb = QueryBuilders.matchAllQuery();

        SearchResponse sr = getClient().prepareSearch(indexName)
                .setQuery(qb)
                .setFetchSource(new String[]{fieldName}, null) // 指定要返回的字段
                .setSize(10000)
                .get();

        return sr.getHits();
    }

    /**
     * 查询所有 matchAll
     * @param indexName
     * @param from
     * @param size
     * @return
     */
    public static SearchHits getMatchAll(String indexName, String fieldName, int from, int size){
        QueryBuilder qb = QueryBuilders.matchAllQuery();

        SearchResponse sr = getClient().prepareSearch(indexName)
                .setQuery(qb)

                .setFetchSource(new String[]{fieldName}, null)
                .setFrom(from)
                .setSize(size).get(); // 每次查size个

        return sr.getHits();
    }

    /**
     * 查询所有 matchAll
     * @param indexName
     * @param from
     * @param size
     * @return
     */
    public static SearchHits getMatchAll(String indexName, List<String> fieldList, int from, int size){
        QueryBuilder qb = QueryBuilders.matchAllQuery();

        SearchRequestBuilder builder = getClient().prepareSearch(indexName);
        String[] includes = new String[fieldList.size()];
        for (int i = 0; i< fieldList.size(); i++){
            includes[i] = fieldList.get(i);
        }

        builder.setFetchSource(includes, null);
        SearchResponse sr = builder
                .setQuery(qb)
                .setFrom(from)
                .setSize(size).get(); // 每次查size个

        return sr.getHits();
    }


    private static HighlightBuilder getHighlightBuilder(String field, boolean needHighLight){
        if (!needHighLight){
            return null;
        }

        //设置高亮显示
        HighlightBuilder highlightBuilder = new HighlightBuilder().field(field).requireFieldMatch(false);
        highlightBuilder.preTags("<span style=\"color:red\">");
        highlightBuilder.postTags("</span>");
        return highlightBuilder;
    }

    private static FunctionScoreQueryBuilder getScoreQueryBuilder(String field, String text){
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .should(
                        //QueryBuilders.matchPhraseQuery(field, text) // 短语匹配，顺序必须一致
                        QueryBuilders.matchQuery(field, text) // 短语匹配，顺序可排
                );

//        FieldValueFactorFunctionBuilder fieldQuery = new FieldValueFactorFunctionBuilder(
//                LawConstants.FOLLOWER_NUM);
        FieldValueFactorFunctionBuilder fieldQuery = new FieldValueFactorFunctionBuilder(
                "");
        // 额外分数=log(1 + factor * follower_num)
        fieldQuery.factor(0.5f);
        fieldQuery.modifier(FieldValueFactorFunction.Modifier.LOG1P);
        // 最终分数=_score+额外分数
        FunctionScoreQueryBuilder functionScoreQueryBuilder = QueryBuilders
                .functionScoreQuery(queryBuilder, fieldQuery)
                .maxBoost(2)
                .boostMode(CombineFunction.MULTIPLY);

        return functionScoreQueryBuilder;
    }

    /**
     * 查询指定 selectField
     * @param indexName
     * @param field
     * @param text
     * @return
     */
    public static SearchHits selectField(String indexName, String field, String text, boolean needHighLight){
        SearchResponse response = getClient()
                .prepareSearch(indexName)
                .setFetchSource(new String[]{field}, null) // 查询指定字段
                .setQuery(getScoreQueryBuilder(field, text))
                .addSort("_score", SortOrder.DESC) //根据分值倒序排列
                .highlighter(getHighlightBuilder(field, needHighLight))
                //searchRequestBuilder.setFrom(0).setSize(100); //设置获取位置个数
                .setPreference("_primary") //设置查询分片,防止震荡问题,生产中可以用 用户id 填入,对同一个用户保证多次查询结果相同
                .get();

        return response.getHits();
    }

    /**
     * 查询指定 selectField
     * @param indexName
     * @param field
     * @param text
     * @param from
     * @param size
     * @return
     */
    public static SearchHits selectField(String indexName, String field, String text, int from, int size, boolean needHighLight){
        QueryBuilder builder = QueryBuilders.matchQuery(field, text);

        SearchResponse sr = getClient().prepareSearch(indexName)
                .setFetchSource(new String[]{field}, null) // 查询指定字段 // 如果没有打开就返回所有字段
                .setQuery(builder)
                .setFrom(from)
                .setSize(size)
                .highlighter(getHighlightBuilder(field, needHighLight))
                .get();

        return sr.getHits();
    }

    /**
     * 通过多个文档id，查询多个文档
     * @param indexName
     * @param typeName
     * @param ids
     * @return
     * @throws IOException
     */
    public static MultiGetResponse selectMultiGet(String indexName, String typeName, String... ids) throws IOException {
        MultiGetRequestBuilder multiGetRequestBuilder = getClient().prepareMultiGet();
        for (String id : ids) {
            multiGetRequestBuilder.add(indexName, typeName, id);
        }
        MultiGetResponse multiGetItemResponses = multiGetRequestBuilder.get();
        return multiGetItemResponses;
    }

    /**
     * 获取Bulk操作
     * @return
     * @throws Exception
     */
    public static BulkRequestBuilder getBulkRequestBuilder() throws Exception {
        BulkRequestBuilder bulkRequestBuilder = getClient().prepareBulk();

        return bulkRequestBuilder;
    }

    /**
     * 查找并删除
     * @param indexName
     * @param field
     * @param text
     * @return
     */
    public static long selectAndDelete(String indexName, String field, String text){
        BulkByScrollResponse response = DeleteByQueryAction.INSTANCE
                .newRequestBuilder(getClient())
                // 查询过虑条件
                .filter(QueryBuilders.matchQuery(field, text))
                .source(indexName)
                .get();
        return response.getDeleted();
    }


    /**
     * 通过索引进行删除
     * @param index
     * @param type
     * @param id
     * @return
     */
    public static DeleteResponse deleteIndexByID(String index,String type,String id) {

        DeleteResponse response = getClient().prepareDelete(index, type, id)
                .execute()
                .actionGet();
        return response;
//        if (response.isFragment()) {
//            System.out.println("索引： " + index + "删除成功！");
//        } else {
//            System.out.println("删除失败！");
//        }
    }

    public static SearchHits selectByDefineScore(String index, String type, String field, String keyWord){
        SearchRequestBuilder searchRequestBuilder = getClient()
                .prepareSearch(index).setTypes(type);
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .should(QueryBuilders.matchPhraseQuery(field, keyWord));

        //Map<String, Object> params = new HashMap<>();
        FieldValueFactorFunctionBuilder fieldQuery = new FieldValueFactorFunctionBuilder(
                "score");
        // 额外分数=log(1+score)
        fieldQuery.factor(0.1f);
        fieldQuery.modifier(FieldValueFactorFunction.Modifier.LOG1P);
        // 最终分数=_score+额外分数
        FunctionScoreQueryBuilder functionScoreQueryBuilder = QueryBuilders
                .functionScoreQuery(queryBuilder, fieldQuery)
                .boostMode(CombineFunction.MULTIPLY);
        //根据分值倒序排列
        searchRequestBuilder.addSort("_score", SortOrder.DESC);
        searchRequestBuilder.setQuery(functionScoreQueryBuilder);
        //设置获取位置个数
        //searchRequestBuilder.setFrom(0).setSize(100);
        //设置查询分片,防止震荡问题,生产中可以用 用户id 填入,对同一个用户保证多次查询结果相同
        searchRequestBuilder.setPreference("233");

        SearchResponse response = searchRequestBuilder.get();
        return response.getHits();
    }

}