package com.lei.util

import java.sql.{Connection, PreparedStatement}
import java.util.Properties

import com.alibaba.druid.pool.DruidDataSourceFactory
import javax.sql.DataSource
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 7:45 上午 2020/4/21
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
class MyJdbcUtil(sql:String ) extends  RichSinkFunction[Array[Any]] {

  val driver = "com.mysql.jdbc.Driver"

  val url = "jdbc:mysql://172.19.180.41:3306/test?useSSL=false"

  val username = "root"

  val password = "1234"

  val maxActive = "20"

  var connection: Connection = null;

  //创建连接
  override def open(parameters: Configuration): Unit = {
    val properties = new Properties()
    properties.put("driverClassName",driver)
    properties.put("url",url)
    properties.put("username",username)
    properties.put("password",password)
    properties.put("maxActive",maxActive)

    val dataSource: DataSource = DruidDataSourceFactory.createDataSource(properties)
    connection = dataSource.getConnection()
  }

  //反复调用
  override def invoke(values: Array[Any]): Unit = {
    val ps: PreparedStatement = connection.prepareStatement(sql )
    println(values.mkString(","))
    for (i <- 0 until values.length) {
      ps.setObject(i + 1, values(i))
    }
    ps.executeUpdate()


  }

  override def close(): Unit = {

    if(connection!=null){
      connection.close()
    }

  }

}
