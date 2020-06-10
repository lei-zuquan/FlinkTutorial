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
  `name` VARCHAR(100) NOT NULL COMMENT '舆情词对应的hashcode',
  `last_update` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间, 必备字段',
  PRIMARY KEY (`id`)
) ENGINE=INNODB DEFAULT CHARSET=utf8;

-- 插入数据
INSERT INTO `t_activities` (`a_id`, `name`) VALUES ('A1', '新人礼包');
INSERT INTO `t_activities` (`a_id`, `name`) VALUES ('A2', '月末活动');
INSERT INTO `t_activities` (`a_id`, `name`) VALUES ('A3', '周末促销');
INSERT INTO `t_activities` (`a_id`, `name`) VALUES ('A4', '年度促销');


-- 活动数据统计
DROP TABLE IF EXISTS `t_activity_counts`;
CREATE TABLE t_activity_counts(
    aid VARCHAR(10),
    event_type INT,
    counts INT,
    PRIMARY KEY (`aid`, `event_type`)
) ENGINE=INNODB DEFAULT CHARSET=utf8;

