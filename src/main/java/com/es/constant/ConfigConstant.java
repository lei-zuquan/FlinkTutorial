package com.es.constant;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: 2020-03-09 15:18
 * @Version: 1.0
 * @Modified By:
 * @Description: 测试Idea中Profiles编译选项
 */

/*
 *   1.加入pom.xml文件依赖
 *         <!-- 导入加载配置文件的依赖-->
 *         <dependency>
 *             <groupId>com.typesafe</groupId>
 *             <artifactId>config</artifactId>
 *             <version>1.2.1</version>
 *         </dependency>
 *
 *   2.添加profiles选项
      <profiles>
        <profile>
            <id>dev</id>
            <activation>
                <!--默认生效的配置组-->
                <activeByDefault>true</activeByDefault>
                <property>
                    <name>env</name>
                    <value>Dev</value>
                </property>
            </activation>
            <build>
                <!--配置文件路径-->
                <resources>
                    <resource>
                        <directory>src/main/resources/dev</directory>
                    </resource>
                </resources>
            </build>
        </profile>
        <profile>
            <id>test</id>
            <activation>

                <property>
                    <name>env</name>
                    <value>Test</value>
                </property>
            </activation>
            <build>
                <!--配置文件路径-->
                <resources>
                    <resource>
                        <directory>src/main/resources/test</directory>
                    </resource>
                </resources>
            </build>
        </profile>
        <profile>
            <id>prod</id>
            <activation>
                <property>
                    <name>env</name>
                    <value>Prod</value>
                </property>
            </activation>
            <build>
                <!--配置文件路径-->
                <resources>
                    <resource>
                        <directory>src/main/resources/prod</directory>
                    </resource>
                </resources>
            </build>
        </profile>
    </profiles>
 *
 */
public class ConfigConstant {

    // 加载配置
    private static Config CONF = ConfigFactory.load();

    public static final String TEST_VALUE = CONF.getString("run.on.test_mode");

    // ES集群名,默认值elasticSearch
    public static final String ES_CLUSTER_NAME = CONF.getString("es.cluster.name");
    // ES集群中节点
    public static final String ES_HOST_NAME = CONF.getString("es.host.name");
    // ES连接端口号
    public static final int ES_TCP_PORT = CONF.getInt("es.host.port");


    public static void main(String[] args) {
        // 1.需要在pom.xml添加maven依赖

        // 加载配置
        //Config conf = ConfigFactory.load();

        //String value = conf.getString("run.on.test");
        System.out.println(TEST_VALUE);
    }
}


