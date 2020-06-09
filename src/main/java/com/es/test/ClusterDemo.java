package com.es.test;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.health.ClusterIndexHealth;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Test;

import java.net.InetAddress;

public class ClusterDemo {

    @Test
    public void test1() throws Exception {

        // 指定ES集群
        // 在配置文件vi /opt/elasticsearch-6.2.4/config/elasticsearch.yml
        // cluster.name: my-application (需要打开)
        // node.name: node-1 (需要打开)
        Settings settings = Settings.builder().put("cluster.name", "my-application").build();
        // 创建访问es服务器的客户端
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("172.19.125.190"), 9300));


        ClusterHealthResponse healths = client.admin().cluster().prepareHealth().get();
        String clusterName = healths.getClusterName();
        System.out.println("clusterName=" + clusterName);

        int numberOfDataNodes = healths.getNumberOfDataNodes();
        System.out.println("numberOfDataNodes=" + numberOfDataNodes);

        int numberOfNodes = healths.getNumberOfNodes();
        System.out.println("numberOfNodes=" + numberOfNodes);

        for (ClusterIndexHealth health : healths.getIndices().values()) {
            String index = health.getIndex();
            int numberOfShards = health.getNumberOfShards();
            int numberOfReplicas = health.getNumberOfReplicas();
            System.out.printf("index=%s, numberOfShards=%d, numberOfReplicas=%d\n", index, numberOfShards, numberOfReplicas);

            ClusterHealthStatus status = health.getStatus();
            System.out.println(status.toString());
        }
    }
}
