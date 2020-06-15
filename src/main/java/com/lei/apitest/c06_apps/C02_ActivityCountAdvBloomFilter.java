package com.lei.apitest.c06_apps;

import com.lei.apitest.util.FlinkUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.hash.BloomFilter;
import org.apache.flink.shaded.guava18.com.google.common.hash.Funnels;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.util.HashSet;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 3:30 下午 2020/6/14
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
/*

// # 创建topic
// kafka-topics --create --zookeeper node-01:2181,node-02:2181,node-03:2181 --replication-factor 2 --partitions 2 --topic activity11
//
// # 创建生产者
// kafka-console-producer --broker-list node-01:9092,node-02:9092,node-03:9092 --topic activity11

各个活动的曝光、点击、参与的人数
各个活动、时间、省份的曝光、点击、参与的次数

将计算好的结果写入到Redis中

1：曝光、2：点击、3：参与
计算活动的点击人数、次数
A1,点击的次数：3, 人数：2

u001,A1,2019-09-02 10:10:11,1,北京市
u002,A1,2019-09-02 10:11:11,1,辽宁省
u001,A1,2019-09-02 10:11:11,2,北京市
u001,A1,2019-09-02 10:11:30,3,北京市
u002,A1,2019-09-02 10:12:11,2,辽宁省
u003,A2,2019-09-02 10:13:11,1,山东省
u003,A2,2019-09-02 10:13:20,2,山东省
u001,A1,2019-09-02 11:11:11,2,北京市

u011,A1,2019-09-02 11:11:11,2,北京市
u012,A1,2019-09-02 11:11:11,2,北京市

bloom过虑器、bitMap、hyperLogLog(size可以计数)

回到公司进行测试功能性

==========================
    实时的分布式全局去重，可以使用Redis效率比较低，我们可以将用户的ID存储到State
    1.定义一个State, State中存储的是HashSet，HashSet的特点是去重，但是HashSet中的数据可能很大甚至内存溢出
    2.优化：定义一个State, 使用Bloom过虑器，但是Bloom过虑器不能计数，还要再定义一个State用来计数



 */
public class C02_ActivityCountAdvBloomFilter {
    public static void main(String[] args) throws Exception {

        ParameterTool parameters = ParameterTool.fromPropertiesFile(args[0]);

        DataStream<String> lines = FlinkUtils.createKafkaStream(
                parameters,
                SimpleStringSchema.class
        );

        // 为了验证程序出现故障时，能否继续之前的数据恢复
        DataStreamSource<String> socketTextStream = FlinkUtils.getEnv().socketTextStream("localhost", 7777);
        socketTextStream.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                if (value.startsWith("null")) {
                    System.out.println( 1 / 0);
                }
                return value;
            }
        }).print();

        // 将数据进行转换
        // u001,A1,2019-09-02 10:10:11,1,北京市
        SingleOutputStreamOperator<C02_ActBean> beanDataStream = lines.map(new MapFunction<String, C02_ActBean>() {
            @Override
            public C02_ActBean map(String line) throws Exception {
                String[] fields = line.split(",");
                String uid = fields[0];
                String aid = fields[1];
                String time = fields[2];
                String date = time.split(" ")[0];
                Integer type = Integer.parseInt(fields[3]);
                String province = fields[3];

                return C02_ActBean.of(uid, aid, date, type, province, 1);
            }
        });

        // 按照指定的条件进行分组
        // 统计次数
        // SingleOutputStreamOperator<C02_ActBean> summed = beanDataStream.keyBy("aid", "time", "type").sum("count");
        // summed.print();
        // SingleOutputStreamOperator<C02_ActBean> summed = beanDataStream.keyBy("aid", "type").sum("count");

        // 统计人数【如果一个活动被一个人点击过一次，以后再点击就不计数了（按照用户ID去重）】
        // 如果是按照用户ID和活动ID两个字段分组，u001,A1这个数据分组后进入到0号SubTask, u005,A1分组后进入到3号SubTask
        // 比如我们想知道A1活动参与的人数
        KeyedStream<C02_ActBean, Tuple> keyed = beanDataStream.keyBy("aid", "type");

        keyed.map(new RichMapFunction<C02_ActBean, Tuple3<String, Integer, Long>>() {
            // 使用HashSet存储不同用户id信息，但是不能够容错；如果subTask挂掉后就从0开始计数
            // HashSet uids = new HashSet<String>();

            // 使用KeyState
            private transient ValueState<BloomFilter> uidState;

            // 一个计数的State
            private transient ValueState<Long> countState;

            @Override
            public void open(Configuration parameters) throws Exception {
                //super.open(parameters);
                // 定义一个状态描述器【布隆过虑器】
                ValueStateDescriptor<BloomFilter> stateDescriptor = new ValueStateDescriptor<>(
                        "uid-state",
                        BloomFilter.class
                );

                // 定义一个状态描述器【次数】
                ValueStateDescriptor<Long> countDescriptor = new ValueStateDescriptor<Long>(
                        "count-state",
                        Long.class
                );

                // 使用RunTimeContext获取状态
                uidState = getRuntimeContext().getState(stateDescriptor);

                countState = getRuntimeContext().getState(countDescriptor);
            }

            @Override
            public Tuple3<String, Integer, Long> map(C02_ActBean bean) throws Exception {
                String uid = bean.uid;
                BloomFilter bloomFilter = uidState.value();
                if (bloomFilter == null) {
                    // 初始化一个bloomFilter
                    bloomFilter = BloomFilter.create(Funnels.unencodedCharsFunnel(), 10000000);
                    countState.update(0L);
                }

                Long counts = countState.value();
                // BloomFilter可以判断一定不包含
                if (!bloomFilter.mightContain(uid)) {
                    // 将当前用户加入到bloomFilter
                    bloomFilter.put(uid);
                    countState.update(counts += 1);
                }

                // 更新用户信息
                uidState.update(bloomFilter);

                return Tuple3.of(bean.aid, bean.type, counts);
            }
        }).print();


        FlinkUtils.getEnv().execute("C02_ActivityCountAdvBloomFilter");
    }
}
