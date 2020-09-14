# Flink 学习
#### 麻烦路过的各位亲给这个项目点个 【star】，太不易了，写了这么多，算是对我坚持下来的一种鼓励吧！
  
  
  
  
# FlinkTutorial **专注大数据Flink流试处理**技术
- 从数据流向角度分析：数据源Source、转换操作Transformation、下沉Sink 
- 从窗口操作角度分析：countWindowAll、countWindow、timeWindowAll、timeWindow滑动窗口 
- 从实际项目角度分析：与kafka、mysql、redis、http、elasticsearch 
- 从容错分析析：StateBackend、checkpoint 

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


### 下沉Sink
- print
- addSink
- writeAsText
- writeAsCsv


# 从窗口操作角度分析
- countWindowAll
- countWindow
- timeWindowAll
- timeWindow滑动窗口 


# 从实际项目角度分析
- 与kafka对接
- mysql对接
- redis对接
- http对接
- elasticsearch对接


# 从容错分析析
- StateBackend
       1. MemoryStateBackend 默认保存到内存中，即检查点保存在JobManager内存中，状态保存在TaskManager内存中  
       2. FsStateBackend 可以是磁盘文件"file:\\\dir_checkp"; 生产环境是: "hdfs://node-01:8020/user/dev/sqoop/flink_state_backend"  
       3. RocksDBStateBackend 将正在运行中的状态数据保存在RocksDB数据库中    
- checkpoint





