package com.lei.domain;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: 2020-05-25 15:15
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
public class J_SensorReading {
    public String id;
    public Long timestamp;
    public Double temperature;

    public J_SensorReading(String id, Long timestamp, Double temperature) {
        this.id = id;
        this.timestamp = timestamp;
        this.temperature = temperature;
    }


    @Override
    public String toString() {
        return "J_SensorReading{" +
                "id='" + id + '\'' +
                ", timestamp=" + timestamp +
                ", temperature=" + temperature +
                '}';
    }
}
