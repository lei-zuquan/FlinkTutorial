package com.lei.apitest.c06_apps;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.lei.apitest.c06_apps.pojo.OrderDetail;
import com.lei.apitest.c06_apps.pojo.OrderMain;
import com.lei.apitest.util.FlinkUtils;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 10:27 上午 2020/6/14
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */

/*
    订单明细表和订单主表进行JOIN

    使用EventTime划分滚动窗口

    使用CoGroup实时左外连接

    TODO 没有JOIN上的数据单独处理

    TODO 使用 canal实时检测mysql表中的数据变化，如果有新数据就向kafka发送数据

    TODO 实际操作
    canal对数据进行提取
    bin/stop.sh
    cd conf/
    cd example/
    rm -r h2.mv.db
    rm -rf meta.dat
    vi instance.properties
    修改canal.mq.dynamicTopic=ordermain:doit\\.ordermain,orderdetail:doit\\.orderdetail
    bin/startup.sh

    事先建立好两个topic
        // # topic:activity10 分区3，副本2
        // # 创建topic
        // kafka-topics --create --zookeeper node-01:2181,node-02:2181,node-03:2181 --replication-factor 2 --partitions 2 --topic ordermain
        // kafka-topics --create --zookeeper node-01:2181,node-02:2181,node-03:2181 --replication-factor 2 --partitions 2 --topic orderdetail
        //
        // # 创建生产者
        // kafka-console-producer --broker-list node-01:9092,node-02:9092,node-03:9092 --topic ordermain
    创建mysql表
        ordermain
        orderdetail


    运行Flink任务，向main方法传入参数：
    /Users/leizuquan/IdeaProjects/FlinkTutorial/conf/config3.properties


    向mysql插入数据：
    INSERT INTO ordermain (oid, create_time, total_money, status, update_time, uid, province) VALUES (5001, NOW(), 2000.0 1, NOW(), 8888, '北京市');
    INSERT INTO orderdetail (order_id, category_id, sku, money, amount, create_time, update_time) VALUES (5001, 1, 10001, 500, 2, NOW(), NOW());
    INSERT INTO orderdetail (order_id, category_id, sku, money, amount, create_time, update_time) VALUES (5001, 2, 20001, 100, 1, NOW(), NOW());

    INSERT INTO ordermain (oid, create_time, total_money, status, update_time, uid, province) VALUES (28001, NOW(), 2000.0 1, NOW(), 9999, '北京市');
    INSERT INTO orderdetail (order_id, category_id, sku, money, amount, create_time, update_time) VALUES (28001, 1, 10001, 500, 2, NOW(), NOW());
    INSERT INTO orderdetail (order_id, category_id, sku, money, amount, create_time, update_time) VALUES (28001, 2, 20001, 100, 1, NOW(), NOW());

    INSERT INTO ordermain (oid, create_time, total_money, status, update_time, uid, province) VALUES (29001, NOW(), 2000.0 1, NOW(), 9999, '北京市');
    INSERT INTO orderdetail (order_id, category_id, sku, money, amount, create_time, update_time) VALUES (29001, 1, 10001, 500, 2, NOW(), NOW());
    INSERT INTO orderdetail (order_id, category_id, sku, money, amount, create_time, update_time) VALUES (29001, 2, 20001, 100, 1, NOW(), NOW());

 */
public class C01_OrderJoin {
    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromPropertiesFile(args[0]);

        long windowTime = parameters.getLong("window.time", 2000);
        long delayTime = parameters.getLong("delay.time", 1000);

        StreamExecutionEnvironment env = FlinkUtils.getEnv();
        // 为了简化，我们做侧输出流时设置并行度为1，生产环境不能设置为1
        env.setParallelism(1);

        // 设置使用EventTime作为时间标准
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<String> orderDetailStream = FlinkUtils.createKafkaStream(
                parameters,
                parameters.getRequired("order.detail.topics"),
                parameters.getRequired("order.detail.group.id"),
                SimpleStringSchema.class
        );

        // 提交EventTime并生成WaterMark
        // 底层方法，实现的是FlatMap加上Filter的功能
        SingleOutputStreamOperator<OrderDetail> orderDetailBeanDStream = orderDetailStream.process(new ProcessFunction<String, OrderDetail>() {
            @Override
            public void processElement(String value, Context ctx, Collector<OrderDetail> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    // 获取操作类型
                    String type = jsonObject.getString("type");
                    // 活动data
                    JSONArray data = jsonObject.getJSONArray("data");
                    for (int i = 0; i < data.size(); i++) {
                        OrderDetail orderDetail = data.getObject(i, OrderDetail.class);
                        if (type.equals("INSET") || type.equals("UPDATE")) {
                            orderDetail.setType(type);
                            out.collect(orderDetail);
                        }
                    }
                } catch (Exception e) {
                    //e.printStackTrace();
                    // TODO 记录有问题的数据
                }
            }
        });

        // 提取EventTime并生成WaterMark
        SingleOutputStreamOperator<OrderDetail> orderDetailWithWaterMark = orderDetailBeanDStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<OrderDetail>(Time.milliseconds(delayTime)) {
            @Override
            public long extractTimestamp(OrderDetail element) {
                return element.getUpdate_time().getTime();
            }
        });

        DataStream<String> orderMainStream = FlinkUtils.createKafkaStream(
                parameters,
                parameters.getRequired("order.main.topics"),
                parameters.getRequired("order.main.group.id"),
                SimpleStringSchema.class
        );

        SingleOutputStreamOperator<OrderMain> orderMainBeanDStream = orderMainStream.process(new ProcessFunction<String, OrderMain>() {
            @Override
            public void processElement(String value, Context ctx, Collector<OrderMain> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    // 获取操作类型
                    String type = jsonObject.getString("type");
                    // 活动data
                    JSONArray data = jsonObject.getJSONArray("data");
                    for (int i = 0; i < data.size(); i++) {
                        OrderMain orderMain = data.getObject(i, OrderMain.class);
                        if (type.equals("INSET") || type.equals("UPDATE")) {
                            orderMain.setType(type);
                            out.collect(orderMain);
                        }
                    }
                } catch (Exception e) {
                    //e.printStackTrace();
                    // TODO 记录有问题的数据
                }
            }
        });


        SingleOutputStreamOperator<OrderMain> orderMainWithWaterMark = orderMainBeanDStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<OrderMain>(Time.milliseconds(delayTime)) {
            @Override
            public long extractTimestamp(OrderMain element) {
                return element.getUpdate_time().getTime();
            }
        });

        // 明细表作为左右
        // 左表数据Join右表数据，右表数据迟到了，没有join上：再查库关联右表数据
        // 左表数据迟到：使用侧输出流获取迟到的数据
        // 得到数所原宽表
        DataStream<Tuple2<OrderDetail, OrderMain>> results = orderDetailWithWaterMark.coGroup(orderMainWithWaterMark)
                .where(new KeySelector<OrderDetail, Long>() {
                    @Override
                    public Long getKey(OrderDetail value) throws Exception {
                        return value.getOrder_id();
                    }
                })
                .equalTo(new KeySelector<OrderMain, Long>() {
                    @Override
                    public Long getKey(OrderMain value) throws Exception {
                        return value.getOid();
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.milliseconds(windowTime)))
                .apply(new CoGroupFunction<OrderDetail, OrderMain, Tuple2<OrderDetail, OrderMain>>() {
                    @Override
                    public void coGroup(Iterable<OrderDetail> first, Iterable<OrderMain> second, Collector<Tuple2<OrderDetail, OrderMain>> out) throws Exception {
                        // 先循环左表的数据
                        for (OrderDetail orderDetail : first) {
                            boolean isJoined = false;
                            for (OrderMain orderMain : second) {
                                out.collect(Tuple2.of(orderDetail, orderMain));
                                isJoined = true;
                            }

                            if (!isJoined) {
                                out.collect(Tuple2.of(orderDetail, null));
                            }
                        }
                    }
                });

        results.print();


        FlinkUtils.getEnv().execute("C01_OrderJoin");
    }
}
