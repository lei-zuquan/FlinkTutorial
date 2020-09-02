package com.lei.apitest.c05_project;

import com.lei.apitest.c05_project.domain.ActivityBean;
import com.lei.apitest.util.FlinkUtilsV1;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

/**
 * @Author:
 * @Date: Created in 10:03 上午 2020/6/8
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
/*


需要导入mysql驱动：
        <dependency>
            <groupId>mysql</groupId>
            <artifactId>mysql-connector-java</artifactId>
            <version>5.1.44</version>
        </dependency>

0.mysql建立表及插入数据
DROP DATABASE IF EXISTS `flink_big_data`; -- 库名与项目名保持一致
CREATE DATABASE IF NOT EXISTS `flink_big_data` DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci;

USE `flink_big_data`;

-- 活动列表
DROP TABLE IF EXISTS `t_activities`;
CREATE TABLE `t_activities` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键id, 必备字段',
  `gmt_create` DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间, 必备字段',
  `gmt_modified` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间, 必备字段',

  `a_id` VARCHAR(100) NOT NULL COMMENT '活动id',
  `name` VARCHAR(100) NOT NULL COMMENT '活动名称',
  `last_update` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间, 必备字段',
  PRIMARY KEY (`id`)
) ENGINE=INNODB DEFAULT CHARSET=utf8;

-- 插入数据
INSERT INTO `t_activities` (`a_id`, `name`) VALUES ('A1', '新人礼包');
INSERT INTO `t_activities` (`a_id`, `name`) VALUES ('A2', '月末活动');
INSERT INTO `t_activities` (`a_id`, `name`) VALUES ('A3', '周末促销');
INSERT INTO `t_activities` (`a_id`, `name`) VALUES ('A4', '年度促销');

1.启动Kafka的topic


2.启动C01_QueryActivityName


3.通过向Kafka-producer生产数据
u001,A1,2019-09-02 10:10:11,1,北京市
u001,A2,2019-09-02 10:10:11,1,北京市
u001,A3,2019-09-02 10:10:11,1,北京市
u001,A4,2019-09-02 10:10:11,1,北京市
u002,A1,2019-09-02 10:11:11,1,辽宁省
u001,A1,2019-09-02 10:11:11,2,北京市
u001,A1,2019-09-02 10:11:30,3,北京市
u002,A1,2019-09-02 10:12:11,2,辽宁省
u001,A1,2019-09-02 10:10:11,1,北京市
u001,A1,2019-09-02 10:10:11,1,北京市
u001,A1,2019-09-02 10:10:11,1,北京市
u001,A1,2019-09-02 10:10:11,1,北京市

希望Flink去MySQL查询此用户得到的礼包匹配数据
u001,新人礼包,2019-09-02 10:10:11,1,北京市
u002,新人礼包,2019-09-02 10:11:11,1,辽宁省

 */
public class C01_QueryActivityName {
    public static void main(String[] args) throws Exception {
        // topic:activity10 分区3，副本2
        // # 创建topic
        // kafka-topics --create --zookeeper node-01:2181,node-02:2181,node-03:2181 --replication-factor 2 --partitions 3 --topic activity10

        // # 创建生产者
        // kafka-console-producer --broker-list node-01:9092,node-02:9092,node-03:9092 --topic activity10

        // 输入参数：activity10 group_id_flink node-01:9092,node-02:9092,node-03:9092
        DataStream<String> lines = FlinkUtilsV1.createKafkaStream(args, new SimpleStringSchema());

        SingleOutputStreamOperator<ActivityBean> beans = lines.map(new C01_DataToActivityBeanFunction());

        beans.print();

        FlinkUtilsV1.getEnv().execute("C01_QueryActivityName");

    }
}
