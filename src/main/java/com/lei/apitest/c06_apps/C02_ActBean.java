package com.lei.apitest.c06_apps;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 3:42 下午 2020/6/14
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
public class C02_ActBean {

    public String uid;

    // 活动ID
    public String aid;

    public String time;

    // 事件类型
    public Integer type;

    public String province;

    public Integer count;

    public C02_ActBean() {

    }

    public C02_ActBean(String uid, String aid, String time, Integer type, String province) {
        this.uid = uid;
        this.aid = aid;
        this.time = time;
        this.type = type;
        this.province = province;
    }

    public static C02_ActBean of(String uid, String aid, String time, Integer type, String province){
        return new C02_ActBean(uid, aid, time, type, province);
    }

    public C02_ActBean(String uid, String aid, String time, Integer type, String province, Integer count) {
        this.uid = uid;
        this.aid = aid;
        this.time = time;
        this.type = type;
        this.province = province;
        this.count = count;
    }

    public static C02_ActBean of(String uid, String aid, String time, Integer type, String province, Integer count){
        return new C02_ActBean(uid, aid, time, type, province, count);
    }

    @Override
    public String toString() {
        return "ActBean{" +
                "uid='" + uid + '\'' +
                ", aid='" + aid + '\'' +
                ", time='" + time + '\'' +
                ", type=" + type +
                ", province='" + province + '\'' +
                ", count=" + count +
                '}';
    }
}
