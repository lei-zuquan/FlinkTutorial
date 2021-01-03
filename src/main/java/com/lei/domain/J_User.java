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

    // Java Bean 必须实现的方法，信息通过字符串进行拼接
    public static String convertToCsv(J_User user) {
        StringBuilder builder = new StringBuilder();
        builder.append("(");

        // add user.id
        builder.append(user.id);
        builder.append(", ");

        // add user.name
        builder.append("'");
        builder.append(String.valueOf(user.name));
        builder.append("', ");

        // add user.age
        builder.append(user.age);

        builder.append(" )");
        return builder.toString();
    }
}
