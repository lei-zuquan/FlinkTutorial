package com.lei.apitest.c02_transformation;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 6:53 上午 2020/6/7
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */

// 封装数据的Bean

public class C05_WordCounts {
    public String word;
    public Long counts;

    public C05_WordCounts() {
    }

    public C05_WordCounts(String word, Long counts) {
        this.word = word;
        this.counts = counts;
    }

    public static C05_WordCounts of(String word, Long counts){
        return new C05_WordCounts(word, counts);
    }

    @Override
    public String toString() {
        return "C05_WordCounts{" +
                "word='" + word + '\'' +
                ", counts=" + counts +
                '}';
    }

    /*private String word;
    private Long counts;

    // 如果提供了有参构造器，一定要提供一个无参构造器，要不以后反射会出问题
    public C05_WordCounts() {
    }

    public C05_WordCounts(String word, Long counts) {
        this.word = word;
        this.counts = counts;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public Long getCounts() {
        return counts;
    }

    public void setCounts(Long counts) {
        this.counts = counts;
    }

    @Override
    public String toString() {
        return "C05_WordCounts{" +
                "word='" + word + '\'' +
                ", counts=" + counts +
                '}';
    }*/
}
