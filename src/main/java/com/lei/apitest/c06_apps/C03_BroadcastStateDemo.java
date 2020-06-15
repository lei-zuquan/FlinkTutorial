package com.lei.apitest.c06_apps;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.lei.apitest.util.FlinkUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.optimizer.operators.MapDescriptor;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 8:20 上午 2020/6/15
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */

/*


insert into activity values ('A1', '新人礼包', NOW(), NOW());
insert into activity values ('A2', '促销活动', NOW(), NOW());
insert into activity values ('A1', '新人礼包', NOW(), NOW());
insert into activity values ('A1', '新人礼包', NOW(), NOW());


cd /cannal/conf
vi instance.properties
添加规则
canal.mq.dynamicTopic=ordermain:bitdata\\.ordermain,orderdetail:bigdata\\.orderdetail,dic:bigdata\\.activity

创建kafka的topic:
// # 创建topic
// kafka-topics --create --zookeeper node-01:2181,node-02:2181,node-03:2181 --replication-factor 2 --partitions 2 --topic dic

// # 启动消费者读取topic dic

启动canal
bin/startup.sh
如果canal非正常退出，则需要将bin目录下的canal.pid进行删除
rm -rf bin/canal.pid

# 数据库数据发生变化，flink数据能及时感知到
insert into activity values ('A3', '双11活动', NOW(), NOW());

update activity set name = "双12活动" where id ='A3';

delete from activity where id = 'A3';


# 向socket 输入数据
uid01,A1,2020-01-09
uid01,A2,2020-01-09

使用广播变量对维度数据进行广播加快访问速度，特点就是数据量要小；比之前异步IO性能要高；如果数据量较大，还得使用异步IO从hbase、es中加载数据

# 待调试

 */
public class C03_BroadcastStateDemo {
    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromPropertiesFile(args[0]);

        DataStream<String> dicDataStream = FlinkUtils.createKafkaStream(
                parameters,
                "dic",
                UUID.randomUUID().toString(), // 为了每次启动后，都能重新读取一下维度表中的数据，每次启动都变换一下group.id
                SimpleStringSchema.class
        );

        SingleOutputStreamOperator<Tuple3<String, String, String>> tpDataStream = dicDataStream.process(new ProcessFunction<String, Tuple3<String, String, String>>() {

            @Override
            public void processElement(String line, Context ctx, Collector<Tuple3<String, String, String>> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(line);
                    JSONArray jsonArray = jsonObject.getJSONArray("data");
                    String type = jsonObject.getString("type");

                    if ("INSERT".equals(type) || "UPDATE".equals(type) || "DELETE".equals(type)) {
                        for (int i = 0; i < jsonArray.size(); i++) {
                            JSONObject obj = jsonArray.getJSONObject(i);
                            String id = obj.getString("id");
                            String name = obj.getString("name");
                            out.collect(Tuple3.of(id, name, type));
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        });

        // 定义一个广播状态描述器
        MapStateDescriptor<String, String> stateDescriptor = new MapStateDescriptor<>(
                "dic-state",
                String.class,
                String.class
        );

        BroadcastStream<Tuple3<String, String, String>> broadcastDataStream = tpDataStream.broadcast(stateDescriptor);

        // 正常的数据流
        // uid01,A1,2020-01-09
        DataStreamSource<String> lines = FlinkUtils.getEnv().socketTextStream("node-01", 7777);

        SingleOutputStreamOperator<Tuple3<String, String, String>> tp2DataStream = lines.map(new MapFunction<String, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(String line) throws Exception {
                String[] fields = line.split(",");

                return Tuple3.of(fields[0], fields[1], fields[2]);
            }
        });

        // 要关联已经广播出去的数据
        SingleOutputStreamOperator<Tuple4<String, String, String, String>> connected = tp2DataStream.connect(broadcastDataStream)
                .process(new BroadcastProcessFunction<Tuple3<String, String, String>, Tuple3<String, String, String>, Tuple4<String, String, String, String>>() {
                    // 处理要计算的活动数据
                    @Override
                    public void processElement(Tuple3<String, String, String> input, ReadOnlyContext ctx, Collector<Tuple4<String, String, String, String>> out) throws Exception {
                        ReadOnlyBroadcastState<String, String> mapState = ctx.getBroadcastState(stateDescriptor);
                        String uid = input.f0;
                        String aid = input.f1;
                        String date = input.f2;

                        // 根据活动ID到广播的stateMap中关联对应的数据
                        String name = mapState.get(aid);

                        // 关联后的数据输出
                        out.collect(Tuple4.of(uid, aid, name, date));

                    }

                    // 处理规则数据的
                    @Override
                    public void processBroadcastElement(Tuple3<String, String, String> tp, Context ctx, Collector<Tuple4<String, String, String, String>> out) throws Exception {
                        String id = tp.f0;
                        String name = tp.f1;
                        String type = tp.f2;
                        // 新来一条规则数据就建规则数据添加到内存
                        BroadcastState<String, String> mapState = ctx.getBroadcastState(stateDescriptor);
                        // 修改在每一个Slot中State中的数据
                        if ("DELETE".equals(type)) {
                            mapState.remove(id);
                        } else {
                            mapState.put(id, name);
                        }

                        Iterator<Map.Entry<String, String>> iterator = mapState.iterator();
                        while (iterator.hasNext()) {
                            Map.Entry<String, String> next = iterator.next();
                            System.out.println("key: " + next.getKey() + " value: " + next.getValue());
                        }
                    }
                });

        connected.print();

        dicDataStream.print();

        FlinkUtils.getEnv().execute("C03_BroadcastStateDemo");
    }



































}
