package com.lei.apitest.c07_sql_api;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 10:50 下午 2020/6/15
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
public class C01_WordCount {
    public String word;
    public Long counts;

    public C01_WordCount() {
    }

    public C01_WordCount(String word, Long counts) {
        this.word = word;
        this.counts = counts;
    }

    @Override
    public String toString() {
        return "WordCount{" +
                "word='" + word + '\'' +
                ", counts='" + counts + '\'' +
                '}';
    }

}
