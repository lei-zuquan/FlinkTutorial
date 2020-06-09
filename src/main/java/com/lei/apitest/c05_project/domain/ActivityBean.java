package com.lei.apitest.c05_project.domain;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 10:15 上午 2020/6/8
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
public class ActivityBean {

    public String uid; // userId
    public String aid; // activityId
    public String activityName;
    public String time;
    public int eventType;
    public double longitude;
    public double latitude;
    public String province;
    public int count = 1;

    public ActivityBean() {
    }

    public ActivityBean(String uid, String aid, String activityName, String time, int eventType, String province) {
        this.uid = uid;
        this.aid = aid;
        this.activityName = activityName;
        this.time = time;
        this.eventType = eventType;
        this.province = province;
    }

    public ActivityBean(String uid, String aid, String activityName, String time, int eventType, double longitude, double latitude, String province) {
        this.uid = uid;
        this.aid = aid;
        this.activityName = activityName;
        this.time = time;
        this.eventType = eventType;
        this.longitude = longitude;
        this.latitude = latitude;
        this.province = province;
    }

    @Override
    public String toString() {
        return "ActivityBean{" +
                "uid='" + uid + '\'' +
                ", aid='" + aid + '\'' +
                ", activityName='" + activityName + '\'' +
                ", time='" + time + '\'' +
                ", eventType=" + eventType +
                ", longitude=" + longitude +
                ", latitude=" + latitude +
                ", province='" + province + '\'' +
                ", count=" + count +
                '}';
    }

    public static ActivityBean of(String uid, String aid, String activityName, String time, int eventType, String province) {
        return new ActivityBean(uid, aid, activityName, time, eventType, province);
    }

    public static ActivityBean of(String uid, String aid, String activityName, String time, int eventType, double longitude, double latitude, String province) {
        return new ActivityBean(uid, aid, activityName, time, eventType, longitude, latitude, province);
    }
}
