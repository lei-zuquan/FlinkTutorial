package com.lei.apitest.c01_value_state;

import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: 2020-06-03 14:34
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
public class J02_ListStateOperate {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        //默认checkpoint功能是disabled的，想要使用的时候需要先启用//默认checkpoint功能是disabled的，想要使用的时候需要先启用

        // 每隔1000 ms进行启动一个检查点【设置checkpoint的周期】
        env.enableCheckpointing(1000);
        // 高级选项：
        // 设置模式为exactly-once （这是默认值）
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 确保检查点之间有至少500 ms的间隔【checkpoint最小间隔】
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        // 检查点必须在一分钟内完成，或者被丢弃【checkpoint的超时时间】
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        // 同一时间只允许进行一个检查点
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint【详细解释见备注】

        /**
         * ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION:表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
         * ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION: 表示一旦Flink处理程序被cancel后，会删除Checkpoint数据，只有job执行失败的时候才会保存checkpoint
         */
        env.getCheckpointConfig().enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.fromCollection(Arrays.asList(
                new Element(1L, 3d),
                new Element(1L, 5d),
                new Element(1L, 7d),
                new Element(2L, 4d),
                new Element(2L, 2d),
                new Element(2L, 6d)
        )).keyBy(t -> t.key)
                .flatMap(new J_CountWindowAverageWithList())
                .print();


        env.execute("J02_ListStateOperate");
    }
}

class Element{
    public Long key;
    public Double value;

    public Element(Long key, Double value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public String toString() {
        return "Element{" +
                "key=" + key +
                ", value=" + value +
                '}';
    }
}
/**
 * 自定义RichFlatMapFunction 可以有更多的方法给我们用
 */
class J_CountWindowAverageWithList extends RichFlatMapFunction<Element, Element>{

    //定义我们历史所有的数据获取
    private ListState<Element> elementsByKey = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        //初始化获取历史状态的值，每个key对应的所有历史值，都存储在list集合里面了
        ListStateDescriptor listState = new ListStateDescriptor<Element>("listState", Element.class);
        elementsByKey = getRuntimeContext().getListState(listState);
    }

    /**
     * flatMap方法  ==》 可以往每个相同key里面塞一些数据  ，塞到一个list集合里面去
     * @param element
     * @param out
     */
    @Override
    public void flatMap(Element element, Collector<Element> out) throws Exception {
        //获取当前key的状态值  通过调用get方法，可以获取状态值   update方法，更新状态值，clear方法，清楚状态值
        Iterable<Element> currentState = elementsByKey.get();

        //如果初始状态为空，那么就进行初始化，构造一个空的集合出来，准备用于存储后续的数据
        if(currentState == null){
            elementsByKey.addAll(Collections.emptyList());
        }
        //添加元素  每个相同key，对应一个state   ==》 ListState  ==》List（Tuple2,Tuple2,Tuple2）
        elementsByKey.add(element);


        //import scala.collection.JavaConverters._
        //将java的集合转换成为scala的集合
        Iterator<Element> allElements = elementsByKey.get().iterator();

        //将集合当中的元素获取到
        List<Element> allElementList= IteratorUtils.toList(allElements);

        if(allElementList.size() >= 3){
            Long count = 0L;  //用于统计每个相同key的数据，有多少条
            Double sum = 0d;   //用于统计每个相同key的数据，累加和是多少
            for (Element eachElement : allElementList) {
                count +=1;
                sum += eachElement.value;
            }

            //将结果进行输出
            out.collect(new Element(element.key, sum/count));

            //  elementsByKey.clear()

        }
    }
}