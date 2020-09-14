# FlinkTutorial **专注大数据Flink流试处理**技术
从数据流向角度分析：数据源Source、转换操作Transformation、下沉Sink
从窗口操作角度分析：countWindowAll、countWindow、timeWindowAll、timeWindow滑动窗口
从实际项目角度分析：与kafka、mysql、redis、http、elasticsearch
从容错分析析：StateBackend、checkpoint

# 从数据流向角度分析
### 数据源Source
socket通信、本地创建数据、从磁盘文件、从Kafka消息中间件中读取

### 转换操作Transformation
- map: 取一个元素并生产一个元素，一个映射函数
- flatMap: 取一个元素并产生零个、一个或多个元素
- filter: 为每个元素评估一个布尔函数，并保留该函数返回true的布尔函数
- keyBy: 从逻辑上将流划分为不相交的分区，具有相同键的所有记录都分配给同一分区。在内部，keyBy() 是通过哈希分区实现的
- reduce: 对key-value数据进行“滚动”压缩。将当前元素与最后一个减少的值合并，并发出新值
- fold: 带有初始值的key-value数据流上的“滚动”折叠。将当前元素与上一个折叠值组合在一起并发出新值
- Aggregations: 在key-value 数据流上滚动聚合。min和minBy之间的区别是min返回最小值，而minBy返回在此字段中具有最小值的元素（与max和maxBy相同）
* sum
* min
* max
* minBy
* maxBy
- window
- union
- connect
- split
- select


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
- checkpoint





