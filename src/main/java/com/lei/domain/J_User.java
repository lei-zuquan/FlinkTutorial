package com.lei.domain;

/**
 * @Author:
 * @Date: 2021-01-03 12:52
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
public class J_User {
    public int id;
    public String name;
    public int age;

    public J_User(int id, String name, int age) {
        this.id = id;
        this.name = name;
        this.age = age;
    }

    public static J_User of(int id, String name, int age) {
        return new J_User(id, name, age);
    }
}
