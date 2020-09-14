# FlinkTutorial **专注大数据Flink流试处理**技术
从数据流向角度分析：数据源Source、转换操作Transformation、下沉Sink
从窗口操作角度分析：countWindowAll、countWindow、timeWindowAll、timeWindow滑动窗口
从实际项目角度分析：与kafka、mysql、redis、http、elasticsearch
从容错分析析：StateBackend、checkpoint

# 从数据流向角度分析
### 数据源Source
socket通信、本地创建数据、从磁盘文件、从Kafka消息中间件中读取

### 转换操作Transformation
- map      \t\t取一个元素并生产一个元素，一个映射函数
- flatMap
- fliter
- keyBy
- reduce
- fold
- Aggregations
+ sum
+ min
+ max
+ minBy
+ maxBy
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





