package com.lei.apitest.c02_transformation;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 7:23 上午 2020/6/7
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
public class C06_OrderBean {

    public String province;
    public String city;
    public Double money;

    public C06_OrderBean() {
    }

    public C06_OrderBean(String province, String city, Double money) {
        this.province = province;
        this.city = city;
        this.money = money;
    }

    public static C06_OrderBean of(String province, String city, Double money) {
        return new C06_OrderBean(province, city, money);
    }

    @Override
    public String toString() {
        return "C06_OrderBean{" +
                "province='" + province + '\'' +
                ", city='" + city + '\'' +
                ", money=" + money +
                '}';
    }


}
