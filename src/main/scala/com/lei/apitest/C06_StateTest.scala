package com.lei.apitest

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 7:45 下午 2020/4/21
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */

/*
      连续数据中如果温度上升，我们判定为有异常，进行报警提示

 */

/*
    ProcessFunctionAPI(底层API)
    我们之前学习的转换算子是无法访问事件的时间戳信息和水位线信息的。而这在一些应用场景下，极为重要。
    例如：MapFunction这样的map转换算子就无法访问时间戳或者当前事件的事件时间。

    基于此，DataStream API提供了一系列的Low-Level转换算子。可以访问时间戳、watermark以及注册
    定时事件。还可以输出特定的一些事件，例如超时事件等。
    ProcessFunction用来构建事件驱动的应用以及实现自定义的业务逻辑（使用之前的window函数和转换算子
    无法实现）。例如：Flink SQL就是使用Process Function实现的

    Flink 提供了8个ProcessFunction:
        ProcessFunction
        KeyedProcessFunction
        CoProcessFunction
        ProcessJoinFunction
        BroadcastProcessFunction
        KeyedBroadcastProcessFunction
        ProcessWindowFunction
        ProcessAllWindowFunction

 */

/*
      温度传感器中，连续两个温度差值超过了10度，则判定为异常，以下就是测试数据及结果
          input data> SensorReading(sensor_1,1547718199,35.0)
          flatMap差值超过阈值：> (sensor_1,0.0,35.0)
          差值超过阈值：> (sensor_1,0.0,35.0)
          input data> SensorReading(sensor_6,1547718201,18.0)
          差值超过阈值：> (sensor_6,0.0,18.0)
          flatMap差值超过阈值：> (sensor_6,0.0,18.0)
          input data> SensorReading(sensor_1,1547718199,44.0)
          input data> SensorReading(sensor_6,1547718201,28.0)
          input data> SensorReading(sensor_1,1547718199,55.0)
          差值超过阈值：> (sensor_1,44.0,55.0)
          flatMap差值超过阈值：> (sensor_1,44.0,55.0)
 */
object C06_StateTest {
  def main(args: Array[String]): Unit = {


    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 默认时间语义是：processes time
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // watermark产生的事件间隔(每n毫秒)是通过ExecutionConfig.setAutoWatermarkInterval(...)来定义的
    //env.getConfig.setAutoWatermarkInterval(100L) // 默认200毫秒

    // 默认env是不开启checkpoint
    //env.enableCheckpointing(1000) // 参加代表间隔多久checkpoint
    // env 设置状态后端
    // env.setStateBackend(new MemoryStateBackend())
    // env.setStateBackend(new FsStateBackend(""))
    // 如果设置RocksDB还需要引入pom依赖
    /*
            <dependency>
                <groupId>org.apache.flink</groupId>
                <artifactId>flink-statebackend-rocksdb_2.11</artifactId>
                <version>1.7.2</version>
            </dependency>
     */
    //env.setStateBackend(new RocksDBStateBackend(""))

    // source
    val inputStream: DataStream[String] = env.socketTextStream("localhost", 7777)

    // Transform操作
    val dataStream: DataStream[SensorReading] = inputStream.map(data => {
      val dataArray: Array[String] = data.split(",")
      // 转成String 方便序列化输出
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })

      /*
       延迟一秒钟上涨水位，这样的操作; 比如：timeWindow是10秒，WaterMark设置成1秒，就是当11秒的数据过来时，才会触发窗口结束
       然后一次会继续上一次的水位时间封装消费10秒窗口数据，同时也得满足水位线延迟1秒
       */
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(element: SensorReading): Long = {
          element.timestamp * 1000
        }
      })

    dataStream.print("input data")

    // 温度连续上升报警
    //val processedStream: DataStream[String] = dataStream.keyBy(_.id)
    //  .process(new TempIncreAlert06())
    //processedStream.print("温度连续上升报警processedStream:")

    // 状态编程方式一：检测某个传感器温度差值不能超过一定限度，需要按传感器id进行分组
    //val processedTempChangeAlertStream: DataStream[(String, Double, Double)] = dataStream.keyBy(_.id)
    //  .process(new TempChangeAlert(10.0))
    //processedTempChangeAlertStream.print("差值超过阈值：")

    // 状态编程方式二：通过flatMap
    //val flatMapChangeAlertStream: DataStream[(String, Double, Double)] = dataStream.keyBy(_.id)
    //  .flatMap(new TempChangeAlertFlatMap(10.0))
    //flatMapChangeAlertStream.print("flatMap差值超过阈值：")

    // 状态编程方式三：通过flatMapWithState，这是flatMap实现的简化版
    val processedFlatMapWithState: DataStream[(String, Double, Double)] = dataStream.keyBy(_.id)
      .flatMapWithState[(String, Double, Double), Double] {
        // 如果没有状态的话，也就是没有数据来过，那么就将当前数据温度值存入状态
        case (input: SensorReading, None) => (List.empty, Some(input.temperature))
        // 如果有状态，就应该与上次的温度值比较差值，如果大于阈值就输出报警
        case (input: SensorReading, lastTemp: Some[Double]) =>
          val diff = (input.temperature - lastTemp.get).abs
          if (diff > 10.0) {
            (List((input.id, lastTemp.get, input.temperature)), Some(input.temperature))
          } else {
            (List.empty, Some(input.temperature))
          }
      }
    processedFlatMapWithState.print("flatMapWithState差值超过阈值：")





    env.execute("window test")

  }

}

/*
    KeyedProcessFunction

    KeyedProcessFunction 用来操作 KeyedStream。KeyedProcessFunction 会处理流
    的每一个元素，输出为0个、1个或者多个元素。所有的Process Function都继承自
    RichFunction接口，所以都有open()、close()和getRuntimeContext()等方法。而
    KeyedProcessFunction[KEY, IN, OUT] 还额外提供了两个方法：
        precessElement(v: IN, ctx: Context, out: Collect[OUT])
            流中的每一个元素
            都会调用这个方法，调用结果将会放在Collector数据类型中输出。Context
            可以访问元素的时间戳，元素的key，以及TimerService时间服务（可以访问WaterMark）。
            Context 还可以将结果输出到别的流（side outputs)。
        onTimer(timestamp:Long, ctx: OnTimerContext, out: Collector[OUT])
            是一个回调函数(捕捉事件)。当之前注册的定时器触发时调用。参数timestamp为定时器所设定
            的触发的时间戳。Collector为输出结果的集合。OnTimerContext和processElement
            的Context参数一样，提供了上下文的一些信息，例如定时器触发的时间信息（事件时间
            或者处理时间）。

    TimerService和定时器（Timers)
        Context和OnTimerContext所持有的TimerService对象拥有以下方法：
            currentProcessingTime():Long返回当前处理时间
            currentWatermark():Long返回当前watermark的时间戳
            registerProcessingTimeTimer(timestamp: Long): Unit会注册当前key的
                processing time的定时器。当processing time到达定时时间时，触发timer.
            registerEventTimeTimer(timestamp:Long):Unit会注册不前key的event time
                定时器。当水位线大于等于定时器注册的时间时，触发定时器执行回调函数。
            deleteProcessingTimeTimer(timestamp: Long):Unit删除之前注册处理时间定时器。
                如果没有这个时间戳的定时器，则不执行。
            deleteEventTimeTimer(timestamp: Long):Unit删除之前注册的事件时间定时器，
                如果没有此时间戳的定时器，则不执行。
       当定时器timer触发时，会执行回调函数onTimer()。注意定时器timer只能在keyed streams上面使用

 */

private class TempIncreAlert06() extends KeyedProcessFunction[String, SensorReading, String]{

  // 定义一个状态，用来保存上一个数据的温度值。将之前的数据保存到状态里
  lazy val lastTemp: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))
  // 定义一个状态，用来保存定时器的时间戳
  lazy val currentTimer: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("currentTimer", classOf[Long]))


  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, collector: Collector[String]): Unit = {
    // 先取出上一个温度值
    val preTemp = lastTemp.value()
    // 更新温度值
    lastTemp.update(value.temperature)

    val curTimerTs: Long = currentTimer.value()

    // 温度上升且没有设过定时器，则注册定时器
    if (preTemp > value.temperature || preTemp == 0.0) {
      // 如果温度下降，或是第一条数据，删除定时器并清空状态
      ctx.timerService().deleteProcessingTimeTimer(curTimerTs)
      currentTimer.clear()
    } else if (value.temperature > preTemp && curTimerTs == 0){
      val timerTs: Long = ctx.timerService().currentProcessingTime() + 10000L
      ctx.timerService().registerProcessingTimeTimer(timerTs)
      currentTimer.update(timerTs)
    }
  }


  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    //super.onTimer(timestamp, ctx, out)
    // 输出报警信息
    out.collect(ctx.getCurrentKey + " 温度连续上升")
    currentTimer.clear()
  }
}


class TempChangeAlert(threshold: Double) extends KeyedProcessFunction[String, SensorReading, (String, Double, Double)] {

  // 定义一个状态变量，保存上次的温度值
  lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))

  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, (String, Double, Double)]#Context, out: Collector[(String, Double, Double)]): Unit = {
    // 获取上次的温度值
    val lastTemp: Double = lastTempState.value()
    // 用当前的温度值和上次的求差，如果大于阈值，输出报警信息
    val diff: Double = (value.temperature - lastTemp).abs
    if (diff > threshold) {
      out.collect(value.id, lastTemp, value.temperature)
    }
    lastTempState.update(value.temperature) // 将当前温度值更新至状态变量中进行保存
  }
}


class TempChangeAlertFlatMap(threshold: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)] {

  private var lastTempState: ValueState[Double] = _

  override def open(parameters: Configuration): Unit = {
    // 初始化的时候声明state变量
    lastTempState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))
  }

  override def flatMap(value: SensorReading, out: Collector[(String, Double, Double)]): Unit = {
    // 获取上次的温度值
    val lastTemp: Double = lastTempState.value()
    // 用当前的温度值和上次的求差，如果大于阈值，输出报警信息
    val diff: Double = (value.temperature - lastTemp).abs
    if (diff > threshold) {
      out.collect(value.id, lastTemp, value.temperature)
    }
    lastTempState.update(value.temperature) // 将当前温度值更新至状态变量中进行保存
  }
}
