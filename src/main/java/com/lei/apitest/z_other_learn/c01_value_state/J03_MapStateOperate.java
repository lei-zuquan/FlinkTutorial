package com.lei.apitest.z_other_learn.c01_value_state;

import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: 2020-06-03 15:06
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
public class J03_MapStateOperate {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.fromCollection(Arrays.asList(
                new Element(1L, 3d),
                new Element(1L, 5d),
                new Element(1L, 7d),
                new Element(2L, 4d),
                new Element(2L, 2d),
                new Element(2L, 6d)
        )).keyBy(t -> t.key)
                .flatMap(new J_CountWithAverageMapState())
                .print();

        env.execute("J03_MapStateOperate");
    }
}


class J_CountWithAverageMapState extends RichFlatMapFunction<Element, Element> {

    private MapState<String, Double> mapState = null;


    //初始化获取mapState对象
    @Override
    public void open(Configuration parameters) throws Exception {
        MapStateDescriptor mapStateOperate =
                new MapStateDescriptor<String, Double>("mapStateOperate",String.class,Double.class);
        mapState = getRuntimeContext().getMapState(mapStateOperate);
    }

    @Override
    public void flatMap(Element element, Collector<Element> out) throws Exception {
        //将相同的key对应的数据放到一个map集合当中去，就是这种对应  1 -> List[Map,Map,Map]
        //每次都构建一个map集合
        //每个相同key的数据，都是对应一个map集合  ==》 hello  => Map(hello -> 1,abc -> 2  , ddd -> 3)
        mapState.put(UUID.randomUUID().toString(), element.value);

        //获取map集合当中所有的value，我们每次将数据的value给放到map的value里面去
        Iterator<Double> iterator = mapState.values().iterator();
        List<Double> listState = IteratorUtils.toList(iterator);
        if(listState.size() >=3){
            Long count = 0L;
            Double sum = 0d;
            for(Double eachState : listState){
                count +=1;
                sum += eachState;
            }
            System.out.println("average"+ sum/count);
            out.collect(new Element(element.key,sum/count));
        }
    }
}
