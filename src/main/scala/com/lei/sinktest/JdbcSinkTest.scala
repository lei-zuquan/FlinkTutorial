package com.lei.sinktest

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.lei.apitest.SensorReading
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 7:33 上午 2020/4/21
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */

object JdbcSinkTest {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // source
    val inputStream: DataStream[String] = env.readTextFile("input_dir/sensor.txt")

    // Transform操作
    val dataStream: DataStream[SensorReading] = inputStream.map(data => {
      val dataArray: Array[String] = data.split(",")
      // 转成String 方便序列化输出
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })

    // sink
    dataStream.addSink(new MyJdbcSink())

    env.execute("jdbc sink test")
  }

}

/*
create database test if not exist

create table temperatures (
  sensor varchar(20) not null,
  temp double not null
);

select * from temperatures
 */
class MyJdbcSink() extends RichSinkFunction[SensorReading]{
  // 定义sql连接、预编译器
  var conn: Connection = _
  var insertStmt: PreparedStatement = _
  var updateStmt: PreparedStatement = _

  // 初始化，创建连接和预编译语句；sql语句进行了预编译后，执行时效率会更高
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    val url = "jdbc:mysql://localhost:3306/test"
    val user = "root"
    val password = "1234"
    conn = DriverManager.getConnection(url, user, password)

    insertStmt = conn.prepareStatement("INSERT INTO temperatures (sensor, temp) VALUES (?, ?)")
    updateStmt = conn.prepareStatement("UPDATE temperatures SET temp = ? WHERE sensor = ?")
  }

  // 调用连接，执行sql
  override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
    // 执行更新语句
    updateStmt.setDouble(1, value.temperature)
    updateStmt.setString(2, value.id)
    updateStmt.execute()

    // 如果update没有查到数据，那么执行插入语句
    if (updateStmt.getUpdateCount == 0){
      insertStmt.setString(1, value.id)
      insertStmt.setDouble(2, value.temperature)
      insertStmt.execute()
    }
  }

  // 关闭时做清理工作
  override def close(): Unit = {
    insertStmt.close()
    updateStmt.close()
    conn.close()
  }
}


/*
<!-- https://mvnrepository.com/artifact/mysql/mysql-connector-java -->
<dependency>
    <groupId>mysql</groupId>
    <artifactId>mysql-connector-java</artifactId>
    <version>5.1.44</version>
</dependency>

<dependency>
    <groupId>com.alibaba</groupId>
    <artifactId>druid</artifactId>
    <version>1.1.10</version>
</dependency>
 */