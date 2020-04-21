package com.lei.util

import java.sql.{Connection, PreparedStatement}

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

  var connection: Connection = null;

  //创建连接
  override def open(parameters: Configuration): Unit = {
    //获取连接池对象
    val dataSource: DataSource  = DruidDataSourceFactory.createDataSource(ConfigurationManager.getProp())
    connection = dataSource.getConnection()
    // 一定要注意druid.properties配置文件中的参数名一定要和上表中的名称相一致，如连接数据库的用户名为username，否则会报错。
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
