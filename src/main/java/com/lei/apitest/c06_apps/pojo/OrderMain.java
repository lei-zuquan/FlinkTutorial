package com.lei.apitest.c06_apps.pojo;

import java.util.Date;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 10:44 上午 2020/6/14
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
public class OrderMain {

    private Long oid;
    private Date create_time;
    private Double total_monty;
    private int status;
    private Date update_time;
    private String province;
    private String city;
    // 对数据库的操作类型：INSERT、UPDATE
    private String type;

    public OrderMain() {
    }

    public OrderMain(Long oid, Date create_time, Double total_monty, int status, Date update_time, String province, String city, String type) {
        this.oid = oid;
        this.create_time = create_time;
        this.total_monty = total_monty;
        this.status = status;
        this.update_time = update_time;
        this.province = province;
        this.city = city;
        this.type = type;
    }

    public static OrderMain of(Long oid, Date create_time, Double total_monty, int status, Date update_time, String province, String city, String type){
        return new OrderMain(oid, create_time, total_monty, status, update_time, province, city, type);
    }

    public Long getOid() {
        return oid;
    }

    public void setOid(Long oid) {
        this.oid = oid;
    }

    public Date getCreate_time() {
        return create_time;
    }

    public void setCreate_time(Date create_time) {
        this.create_time = create_time;
    }

    public Double getTotal_monty() {
        return total_monty;
    }

    public void setTotal_monty(Double total_monty) {
        this.total_monty = total_monty;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public Date getUpdate_time() {
        return update_time;
    }

    public void setUpdate_time(Date update_time) {
        this.update_time = update_time;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
