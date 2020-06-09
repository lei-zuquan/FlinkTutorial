package com.es.util.test;

import com.es.util.ESUtil;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;


/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: 2020-03-23 10:44
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */

// 3.接下来，以两种方式插入文档到blog：

public class InsertDemo {
    public static void main(String[] args) throws IOException {
        // 方式一
        // {"id":"1","title":"Java連接ES","content":"abcdefg。","postdate":"2019-03-24 14:38:00","url":"bas.com"}
        String json = "{" +
                "\"id\":\"1\"," +
                "\"title\":\"Java連接ES\"," +
                "\"content\":\"abcdefg。\"," +
                "\"postdate\":\"2019-03-24 14:38:00\"," +
                "\"url\":\"bas.com\"" +
                "}";
        System.out.println(json);

        ESUtil.insertDocument("app_account", "blog", json);

        // 方式二
        XContentBuilder doc = jsonBuilder()
                .startObject()
                .field("id","2")
                .field("title","Java插入数据到ES")
                .field("content","abcedfasdasd")
                .field("postdate","2019-03-24 14:38:00")
                .field("url","bas.com")
                .endObject();
        ESUtil.insertDocument("app_account", "blog", doc);

        /**
         * 上述是插入数据，接下来进行数据的查询
         * GET /app_account/blog/_search
         */

        GetResponse response = ESUtil.selectDocument("app_account", "blog", "1");
    }
}