package com.lei.apitest.z_other_learn.c02_cep;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: 2020-06-05 16:20
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
class J_OrderDetail{
    public String orderId;
    public String status;
    public String orderCreateTime;
    public Double price;

    public J_OrderDetail(String orderId, String status, String orderCreateTime, Double price) {
        this.orderId = orderId;
        this.status = status;
        this.orderCreateTime = orderCreateTime;
        this.price = price;
    }

    @Override
    public String toString() {
        return "J_OrderDetail{" +
                "orderId='" + orderId + '\'' +
                ", status='" + status + '\'' +
                ", orderCreateTime='" + orderCreateTime + '\'' +
                ", price=" + price +
                '}';
    }
}

public class J04_OrderTimeOutCheckCEP {

    private static FastDateFormat format = FastDateFormat.getInstance("yyy-MM-dd HH:mm:ss");

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        DataStreamSource<String> sourceStream = env.socketTextStream("node-01", 7777);

        KeyedStream<J_OrderDetail, String> keyedStream = sourceStream.map(new MapFunction<String, J_OrderDetail>() {
            @Override
            public J_OrderDetail map(String s) throws Exception {
                String[] split = s.split(",");
                return new J_OrderDetail(split[0], split[1], split[2], Double.valueOf(split[3]));
            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<J_OrderDetail>(Time.seconds(5)) {
            @Override
            public long extractTimestamp(J_OrderDetail j_orderDetail) {
                String orderCreateTime = j_orderDetail.orderCreateTime;
                long time = 0;
                try {
                    time = format.parse(orderCreateTime).getTime();
                } catch (Exception ex) {
                    ex.printStackTrace();
                }

                return time;
            }
        }).keyBy(t -> t.orderId);

        Pattern<J_OrderDetail, J_OrderDetail> pattern = Pattern.<J_OrderDetail>begin("start")
                .where(new IterativeCondition<J_OrderDetail>() {
                    @Override
                    public boolean filter(J_OrderDetail j_orderDetail, Context<J_OrderDetail> context) throws Exception {
                        return j_orderDetail.status.equals("1"); // 订单的最开始的状态是1
                    }
                })
                .followedByAny("second") // 第二个条件 订单的状态是2  严格临近模式
                .where(new IterativeCondition<J_OrderDetail>() {
                    @Override
                    public boolean filter(J_OrderDetail j_orderDetail, Context<J_OrderDetail> context) throws Exception {
                        return j_orderDetail.status.equals("2");
                    }
                })
                .within(Time.minutes(15));// 在15分钟之内出现2 即可

        // 4.调用select方法，提取事件序列，超时的事件要做报警提示
        final OutputTag<J_OrderDetail> orderDetailOutputTag = new OutputTag<J_OrderDetail>("orderTimeOut");

        PatternStream<J_OrderDetail> patternStream = CEP.pattern(keyedStream, pattern);
        SingleOutputStreamOperator<J_OrderDetail> selectResultStream =
                patternStream.select(orderDetailOutputTag, new J_OrderTimeoutPatternFunction(), new J_OrderPatternFunction());

        selectResultStream.print();
        // 打印侧输出流数据 过了15分钟还没支付的数据
        selectResultStream.getSideOutput(orderDetailOutputTag).print();

        env.execute("J04_OrderTimeOutCheckCEP");
    }
}

// 订单超时检测
class J_OrderTimeoutPatternFunction implements PatternTimeoutFunction<J_OrderDetail, J_OrderDetail> {

    @Override
    public J_OrderDetail timeout(Map<String, List<J_OrderDetail>> pattern, long l) throws Exception {
        J_OrderDetail detail = pattern.get("start").iterator().next();
        System.out.println("超时订单号为：" + detail);

        return detail;
    }
}

class J_OrderPatternFunction implements PatternSelectFunction<J_OrderDetail, J_OrderDetail> {

    @Override
    public J_OrderDetail select(Map<String, List<J_OrderDetail>> pattern) throws Exception {
        J_OrderDetail detail = pattern.get("second").iterator().next();
        System.out.println("支付成功的订单为：" + detail);
        return detail;
    }
}

/*
* 场景介绍

  * 在我们的电商系统当中，经常会发现有些订单下单之后没有支付，就会有一个倒计时的时间值，提示你在15分钟之内完成支付，如果没有完成支付，那么该订单就会被取消，主要是因为拍下订单就会减库存，但是如果一直没有支付，那么就会造成库存没有了，别人购买的时候买不到，然后别人一直不支付，就会产生有些人买不到，有些人买到了不付款，最后导致商家一件产品都卖不出去

* 需求

*
  * 创建订单之后15分钟之内一定要付款，否则就取消订单

* 订单数据格式如下类型字段说明
  * 订单编号

  * 订单状态

    * 1.创建订单,等待支付
    * 2.支付订单完成
    * 3.取消订单，申请退款
    * 4.已发货
    * 5.确认收货，已经完成

  * 订单创建时间

  * 订单金额

    ~~~
20160728001511050311389390,1,2016-07-28 00:15:11,295
20160801000227050311955990,1,2016-07-28 00:16:12,165
20160728001511050311389390,2,2016-07-28 00:18:11,295
20160801000227050311955990,2,2016-07-28 00:18:12,165
20160728001511050311389390,3,2016-07-29 08:06:11,295
20160801000227050311955990,4,2016-07-29 12:21:12,165
20160804114043050311618457,1,2016-07-30 00:16:15,132
20160801000227050311955990,5,2016-07-30 18:13:24,165
    ~~~


* 规则，出现 1 创建订单标识之后，紧接着需要在15分钟之内出现 2 支付订单操作，中间允许有其他操作
 */