package com.lei.apitest.value_state

import java.lang
import java.util.Collections

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: 2020-05-20 12:39
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */



/*
    作用
      用于保存每个key的历史数据数据成为一个列表

    需求
      使用ListState求取数据平均值

   使用ListState实现平均值求取
   ListState<T> ：这个状态为每一个 key 保存集合的值
        get() 获取状态值
        add() / addAll() 更新状态值，将数据放到状态中
        clear() 清除状态
 */
object ListStateOperate {

  def main(args: Array[String]): Unit = {
    //获取程序的入口类
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //将状态保存到内存当中，不建议
    //env.setStateBackend(new MemoryStateBackend())

    //第二种方式，保存到文件系统 可以使用
    //env.setStateBackend(new FsStateBackend("hdfs://node01:8020/flink/checkDir"))

    //保存到rockDB里面去  推荐优选
    //env.setStateBackend(new RocksDBStateBackend("hdfs://node-01:8020/user/root/flink/checkDir",true))
    /**
     * 配置checkPoint
     */
    import org.apache.flink.streaming.api.CheckpointingMode
    import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
    //默认checkpoint功能是disabled的，想要使用的时候需要先启用//默认checkpoint功能是disabled的，想要使用的时候需要先启用

    // 每隔1000 ms进行启动一个检查点【设置checkpoint的周期】
    env.enableCheckpointing(1000)
    // 高级选项：
    // 设置模式为exactly-once （这是默认值）
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    // 确保检查点之间有至少500 ms的间隔【checkpoint最小间隔】
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    // 检查点必须在一分钟内完成，或者被丢弃【checkpoint的超时时间】
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    // 同一时间只允许进行一个检查点
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    // 表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint【详细解释见备注】

    /**
     * ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION:表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
     * ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION: 表示一旦Flink处理程序被cancel后，会删除Checkpoint数据，只有job执行失败的时候才会保存checkpoint
     */
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    import org.apache.flink.api.scala._
    env.fromCollection(List(
      (1L, 3d),
      (1L, 5d),
      (1L, 7d),
      (2L, 4d),
      (2L, 2d),
      (2L, 6d)
    )).keyBy(_._1)
      .flatMap(new CountWindowAverageWithList)
      .print()
    env.execute()
  }
}


/**
 * 自定义RichFlatMapFunction 可以有更多的方法给我们用
 */
class CountWindowAverageWithList extends RichFlatMapFunction[(Long,Double),(Long,Double)]{
  //定义我们历史所有的数据获取
  private var elementsByKey: ListState[(Long,Double)] = _

  override def open(parameters: Configuration): Unit = {
    //初始化获取历史状态的值，每个key对应的所有历史值，都存储在list集合里面了
    val listState = new ListStateDescriptor[(Long,Double)]("listState",classOf[(Long,Double)])
    elementsByKey = getRuntimeContext.getListState(listState)

  }

  /**
   * flatMap方法  ==》 可以往每个相同key里面塞一些数据  ，塞到一个list集合里面去
   * @param element
   * @param out
   */
  override def flatMap(element: (Long, Double), out: Collector[(Long, Double)]): Unit = {
    //获取当前key的状态值  通过调用get方法，可以获取状态值   update方法，更新状态值，clear方法，清楚状态值
    val currentState: lang.Iterable[(Long, Double)] = elementsByKey.get()

    //如果初始状态为空，那么就进行初始化，构造一个空的集合出来，准备用于存储后续的数据
    if(currentState == null){
      elementsByKey.addAll(Collections.emptyList())
    }
    //添加元素  每个相同key，对应一个state   ==》 ListState  ==》List（Tuple2,Tuple2,Tuple2）
    elementsByKey.add(element)


    import scala.collection.JavaConverters._
    //将java的集合转换成为scala的集合
    val allElements: Iterator[(Long, Double)] = elementsByKey.get().iterator().asScala

    //将集合当中的元素获取到
    val allElementList: List[(Long, Double)] = allElements.toList
    if(allElementList.size >= 3){
      var count = 0L  //用于统计每个相同key的数据，有多少条
      var sum = 0d   //用于统计每个相同key的数据，累加和是多少
      for(eachElement <- allElementList){
        count +=1
        sum += eachElement._2
      }
      //将结果进行输出
      out.collect((element._1,sum/count))

      //  elementsByKey.clear()

    }
  }
}
