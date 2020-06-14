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
public class OrderDetail {
    private Long id;
    private Long order_id;
    private int category_id;
    private String categoryName;
    private Long sku;
    private Double money;
    private int amount;
    private Date create_time;
    private Date update_time;
    // 对数据库的操作类型：INSERT、UPDATE
    private String type;


    public OrderDetail() {
    }

    public OrderDetail(Long id, Long order_id, int category_id, String categoryName, Long sku, Double money, int amount, Date create_time, Date update_time, String type) {
        this.id = id;
        this.order_id = order_id;
        this.category_id = category_id;
        this.categoryName = categoryName;
        this.sku = sku;
        this.money = money;
        this.amount = amount;
        this.create_time = create_time;
        this.update_time = update_time;
        this.type = type;
    }

    public static OrderDetail of(Long id, Long order_id, int category_id, String categoryName, Long sku, Double money, int amount, Date create_time, Date update_time, String type){
        return new OrderDetail(id, order_id, category_id, categoryName, sku, money, amount, create_time, update_time, type);
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getOrder_id() {
        return order_id;
    }

    public void setOrder_id(Long order_id) {
        this.order_id = order_id;
    }

    public int getCategory_id() {
        return category_id;
    }

    public void setCategory_id(int category_id) {
        this.category_id = category_id;
    }

    public String getCategoryName() {
        return categoryName;
    }

    public void setCategoryName(String categoryName) {
        this.categoryName = categoryName;
    }

    public Long getSku() {
        return sku;
    }

    public void setSku(Long sku) {
        this.sku = sku;
    }

    public Double getMoney() {
        return money;
    }

    public void setMoney(Double money) {
        this.money = money;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public Date getCreate_time() {
        return create_time;
    }

    public void setCreate_time(Date create_time) {
        this.create_time = create_time;
    }

    public Date getUpdate_time() {
        return update_time;
    }

    public void setUpdate_time(Date update_time) {
        this.update_time = update_time;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
