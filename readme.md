# Flink 学习链接 —— 都是干货
##### 麻烦路过的各位亲给这个项目点个 【star】，太不易了，写了这么多，算是对我坚持下来的一种鼓励吧！

![在这里插入图片描述](https://img-blog.csdnimg.cn/20200914200238753.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3dlaXhpbl8zMjI2NTU2OQ==,size_16,color_FFFFFF,t_70#pic_center)
<hr style=" border:solid; width:100px; height:1px;" color=#000000 size=1">

![在这里插入图片描述](https://img-blog.csdnimg.cn/20200914202628920.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3dlaXhpbl8zMjI2NTU2OQ==,size_16,color_FFFFFF,t_70#pic_center)

# FlinkTutorial **专注大数据Flink流试处理**技术
- 从数据流向角度分析：数据源Source、转换操作Transformation、下沉Sink 
- 从窗口操作角度分析：countWindowAll、countWindow、timeWindowAll、timeWindow滑动窗口 
- 从实际项目角度分析：与kafka、mysql、redis、http、elasticsearch 
- 从容错分析析：StateBackend、checkpoint 

<hr style=" border:solid; width:100px; height:1px;" color=#000000 size=1">  

# 从数据流向角度分析
### 数据源Source
socket通信、本地创建数据、从磁盘文件、从Kafka消息中间件中读取

### 转换操作Transformation
- **map**: 取一个元素并生产一个元素，一个映射函数
- **flatMap**: 取一个元素并产生零个、一个或多个元素
- **filter**: 为每个元素评估一个布尔函数，并保留该函数返回true的布尔函数
- **keyBy**: 从逻辑上将流划分为不相交的分区，具有相同键的所有记录都分配给同一分区。在内部，keyBy() 是通过哈希分区实现的
- **reduce**: 对key-value数据进行“滚动”压缩。将当前元素与最后一个减少的值合并，并发出新值
- **fold**: 带有初始值的key-value数据流上的“滚动”折叠。将当前元素与上一个折叠值组合在一起并发出新值
- **Aggregations**: 在key-value 数据流上滚动聚合。min和minBy之间的区别是min返回最小值，而minBy返回在此字段中具有最小值的元素（与max和maxBy相同）   
       1. sum 求和  
       2. min 求最小值
       3. max 求最大值 
       4. minBy  
       5. maxBy  
- **window**: 窗口操作
- **union**: 两个或多个数据流的并集，创建一个包含所有流中所有元素的新流。注意：如果将数据流与其自身合并，则在结果流中每个元素将获得两次
- **connect**: “连接”两个保留其类型的数据流。连接允许两个流之间共享状态
- **split**: 根据某种标准将流分成两个或多个流
- **select**: 从拆分流中选择一个或多个流


<hr style=" border:solid; width:100px; height:1px;" color=#000000 size=1">  

### 下沉Sink
- print
- addSink
- writeAsText
- writeAsCsv


<hr style=" border:solid; width:100px; height:1px;" color=#000000 size=1">  

# 从窗口操作角度分析
- countWindowAll
- countWindow
- timeWindowAll
- timeWindow滑动窗口 


<hr style=" border:solid; width:100px; height:1px;" color=#000000 size=1">  

# 从实际项目角度分析
- 与kafka对接
- mysql对接
- redis对接
- http对接
- elasticsearch对接


<hr style=" border:solid; width:100px; height:1px;" color=#000000 size=1">  

# 从容错角度分析
 1. StateBackend  

> MemoryStateBackend 默认保存到内存中，即检查点保存在JobManager内存中，状态保存在TaskManager内存中
> FsStateBackend 可以是磁盘文件"file:\\\dir_checkp"; 生产环境是: "hdfs://node-01:8020/user/dev/sqoop/flink_state_backend"     
> RocksDBStateBackend 将正在运行中的状态数据保存在RocksDB数据库中

 1. Checkpoint

>  Checkpoint 使Flink 的状态具有良好的容错性，通过checkpoint 机制，Flink 可以对作业和计算位置进行恢复。


<hr style=" border:solid; width:100px; height:1px;" color=#000000 size=1">  

# Flink 相关的深度技术博客链接

[1. Flink —— 什么是Flink？](https://blog.csdn.net/weixin_32265569/article/details/108572221)  
[2. 超全干货--Flink思维导图，花了3周左右编写、校对](https://blog.csdn.net/weixin_32265569/article/details/107426591)  
[3. 大解密：Flink恰巧语义一次消费，怎么保证](https://blog.csdn.net/weixin_32265569/article/details/107439769)  
[4. Flink 容错机制 —— CheckPoint【含示例源码】](https://blog.csdn.net/weixin_32265569/article/details/107439769)  
[5. Flink实战 —— 读取Kafka数据并与MySQL数据关联【附源码】](https://blog.csdn.net/weixin_32265569/article/details/108367968)  
[6. Flink —— StateBackend 状态后端](https://blog.csdn.net/weixin_32265569/article/details/108449983)  


<hr style=" border:solid; width:100px; height:1px;" color=#000000 size=1">  

# 文章最后，给大家推荐一些受欢迎的技术博客链接：

[1. JAVA相关的深度技术博客链接](https://blog.csdn.net/weixin_32265569/article/details/107575286)  
[2. Flink 相关技术博客链接](https://blog.csdn.net/weixin_32265569/article/details/108211280)   
[3. Spark 核心技术链接](https://blog.csdn.net/weixin_32265569/article/details/107575521)  
[4. 设计模式 —— 深度技术博客链接](https://blog.csdn.net/weixin_32265569/article/details/108417756)  
[5. 机器学习 —— 深度技术博客链接](https://blog.csdn.net/weixin_32265569/article/details/108417908)  
[6. Hadoop相关技术博客链接](https://blog.csdn.net/weixin_32265569/article/details/107575853)  
[7. 超全干货--Flink思维导图，花了3周左右编写、校对](https://blog.csdn.net/weixin_32265569/article/details/107426591)  
[8. 深入JAVA 的JVM核心原理解决线上各种故障【附案例】](https://blog.csdn.net/weixin_32265569/article/details/107699015)  
[9. 请谈谈你对volatile的理解？--最近小李子与面试官的一场“硬核较量”](https://blog.csdn.net/weixin_32265569/article/details/107425491)  
[10. 聊聊RPC通信，经常被问到的一道面试题。源码+笔记，包懂](https://blog.csdn.net/weixin_32265569/article/details/107425756)  
[11. 深入聊聊Java 垃圾回收机制【附原理图及调优方法】](https://blog.csdn.net/weixin_32265569/article/details/107830848#5.6%20%E6%A1%88%E4%BE%8B)  


<hr style=" border:solid; width:100px; height:1px;" color=#000000 size=1">  


**欢迎扫描下方的二维码或 搜索 公众号“大数据高级架构师”，我们会有更多、且及时的资料推送给您，欢迎多多交流！**

![在这里插入图片描述](https://img-blog.csdnimg.cn/20200914202050131.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3dlaXhpbl8zMjI2NTU2OQ==,size_16,color_FFFFFF,t_70#pic_center)
                                           

     
