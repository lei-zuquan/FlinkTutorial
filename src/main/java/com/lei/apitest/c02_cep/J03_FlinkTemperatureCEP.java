package com.lei.apitest.c02_cep;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: 2020-06-05 10:52
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */

// 在三分钟之内，出现温度高于40度，三次就报警
// 传感器设备mac地址，检测机器mac地址，温度
// 传感器设备mac地址，检测机器mac地址，温度，湿度，气压，数据产生时间
// 00-34-5E-5F-89-A4,00-01-6C-06-A6-29,38,0.52,1.1,2020-03-02 12:20:32

/*
      CEP测试情况不态稳定，SCALA版本可以使用，JAVA版本数据无法进入CEP模式匹配
 */

public class J03_FlinkTemperatureCEP {

    private static FastDateFormat format = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置时间类型，以事件发生时间为准
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 从socket中读取数据
        DataStreamSource<String> sourceStream = env.socketTextStream("node-01", 7777);

        KeyedStream<J_DeviceDetail, String> keyedStream = sourceStream.map(new J03_CEP_MyMapClass())
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<J_DeviceDetail>() {
                    long bound = 60000;
                    long maxTs = Long.MIN_VALUE;
                    @Nullable
                    @Override
                    public Watermark getCurrentWatermark() {
                        return new Watermark(maxTs - bound);
                    }

                    @Override
                    public long extractTimestamp(J_DeviceDetail element, long previousElementTimestamp) {
                        long timeStamp = 0;
                        try {
                            timeStamp = format.parse(element.date).getTime();
                        } catch (Exception ex) {
                            ex.printStackTrace();
                        }
                        maxTs = Long.max(maxTs, timeStamp);

                        return timeStamp;
                    }
                }).keyBy(
                        t -> t.sensorMac
                );

        keyedStream.print();

        Pattern<J_DeviceDetail, J_DeviceDetail> pattern = Pattern.<J_DeviceDetail>begin("start")
                .where(new IterativeCondition<J_DeviceDetail>() {
                    @Override
                    public boolean filter(J_DeviceDetail j_deviceDetail, Context<J_DeviceDetail> context) throws Exception {
                        String temperature = j_deviceDetail.temperature;
                        return Integer.parseInt(temperature) >= 40;
                    }
                }).within(Time.minutes(3));
        /*Pattern<J_DeviceDetail, J_DeviceDetail> pattern = Pattern.<J_DeviceDetail>begin("start")
                .where(new IterativeCondition<J_DeviceDetail>() {
                    @Override
                    public boolean filter(J_DeviceDetail j_deviceDetail, Context<J_DeviceDetail> context) throws Exception {
                        String temperature = j_deviceDetail.temperature;
                        return Integer.parseInt(temperature) >= 40;
                    }
                })
                .followedByAny("second")
                .where(new IterativeCondition<J_DeviceDetail>() {
                    @Override
                    public boolean filter(J_DeviceDetail j_deviceDetail, Context<J_DeviceDetail> context) throws Exception {
                        String temperature = j_deviceDetail.temperature;
                        return Integer.parseInt(temperature) >= 40;
                    }
                })
                .followedByAny("third")
                .where(new IterativeCondition<J_DeviceDetail>() {
                    @Override
                    public boolean filter(J_DeviceDetail j_deviceDetail, Context<J_DeviceDetail> context) throws Exception {
                        String temperature = j_deviceDetail.temperature;
                        return Integer.parseInt(temperature) >= 40;
                    }
                })
                //.timesOrMore(3)
                .within(Time.minutes(3));*/

        PatternStream<J_DeviceDetail> patternStream = CEP.pattern(keyedStream, pattern);

        patternStream.select(new J03_CEP_MyPatternResultFunction()).print();

        env.execute("J03_FlinkTemperatureCEP");
    }
}

class J03_CEP_MyPatternResultFunction implements PatternSelectFunction<J_DeviceDetail, J_AlarmDevice> {
    @Override
    public J_AlarmDevice select(Map<String, List<J_DeviceDetail>>map)throws Exception{
        Iterator<J_DeviceDetail> startIterator=map.get("start").iterator();
//        Iterator<J_DeviceDetail> secondIterator=map.get("second").iterator();
//        Iterator<J_DeviceDetail> thirdIterator=map.get("third").iterator();

        J_DeviceDetail start=startIterator.next();
//        J_DeviceDetail second=secondIterator.next();
//        J_DeviceDetail third=thirdIterator.next();

        System.out.println("第一条数据："+start);
//        System.out.println("第二条数据："+second);
//        System.out.println("第三条数据："+third);

        return new J_AlarmDevice(start.sensorMac,start.deviceMac,start.temperature);
    }
}

class J03_CEP_MyMapClass implements MapFunction<String, J_DeviceDetail> {

    @Override
    public J_DeviceDetail map(String s) throws Exception {
        String[] split = s.split(",");
        return new J_DeviceDetail(split[0], split[1], split[2], split[3], split[4], split[5]);
    }
}

// 定义温度信息pojo
class J_DeviceDetail {
    public String sensorMac;
    public String deviceMac;
    public String temperature;
    public String dampness;
    public String pressure;
    public String date;

    public J_DeviceDetail(String sensorMac, String deviceMac, String temperature, String dampness, String pressure, String date) {
        this.sensorMac = sensorMac;
        this.deviceMac = deviceMac;
        this.temperature = temperature;
        this.dampness = dampness;
        this.pressure = pressure;
        this.date = date;
    }

    @Override
    public String toString() {
        return "J_DeviceDetail{" +
                "sensorMac='" + sensorMac + '\'' +
                ", deviceMac='" + deviceMac + '\'' +
                ", temperature='" + temperature + '\'' +
                ", dampness='" + dampness + '\'' +
                ", pressure='" + pressure + '\'' +
                ", date='" + date + '\'' +
                '}';
    }
}

// 报警的设备信息样例类
class J_AlarmDevice {
    public String sensorMac;
    public String deviceMac;
    public String temperature;

    public J_AlarmDevice(String sensorMac, String deviceMac, String temperature) {
        this.sensorMac = sensorMac;
        this.deviceMac = deviceMac;
        this.temperature = temperature;
    }


    @Override
    public String toString() {
        return "J_AlarmDevice{" +
                "sensorMac='" + sensorMac + '\'' +
                ", deviceMac='" + deviceMac + '\'' +
                ", temperature='" + temperature + '\'' +
                '}';
    }
}


/*
* 场景介绍

  * 现在工厂当中有大量的传感设备，用于检测机器当中的各种指标数据，例如温度，湿度，气压等，并实时上报数据到数据中心，现在需要检测，某一个传感器上报的温度数据是否发生异常。

* 异常的定义

  * 三分钟时间内，出现三次及以上的温度高于40度就算作是异常温度，进行报警输出

* 收集数据如下：

  ~~~
  传感器设备mac地址，检测机器mac地址，温度，湿度，气压，数据产生时间

00-34-5E-5F-89-A4,00-01-6C-06-A6-29,38,0.52,1.1,2020-03-02 12:20:32
00-34-5E-5F-89-A4,00-01-6C-06-A6-29,47,0.48,1.1,2020-03-02 12:20:35
00-34-5E-5F-89-A4,00-01-6C-06-A6-29,50,0.48,1.1,2020-03-02 12:20:38
00-34-5E-5F-89-A4,00-01-6C-06-A6-29,31,0.48,1.1,2020-03-02 12:20:39
00-34-5E-5F-89-A4,00-01-6C-06-A6-29,52,0.48,1.1,2020-03-02 12:20:41
00-34-5E-5F-89-A4,00-01-6C-06-A6-29,53,0.48,1.1,2020-03-02 12:20:43
00-34-5E-5F-89-A4,00-01-6C-06-A6-29,55,0.48,1.1,2020-03-02 12:20:45
~~~
 */
