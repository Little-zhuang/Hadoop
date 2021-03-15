package com.zhuang.zookeeperAPI;


import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.junit.Test;

/**
 * @author Elke
 * @Description 创建永久节点
 * @date 2021/2/8-21:06
 */
public class ZookeeperAPITest {

    @Test
    public void createNode() throws Exception{

//        1:定制一个重试策略
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000,1);
//        2:获取一个客户端对象
        String connection = "192.168.91.100:2181,192.168.91.110:2181,192.168.91.120:2181";
        CuratorFramework client = CuratorFrameworkFactory.newClient(connection, 10000, 10000, retryPolicy);
//        3:开启客户端
        client.start();
//        4:创建一个节点(PERSISTENT是永久节点，EPHEMERAL是临时节点)
        client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath("/hello2","word".getBytes());
//        5:关闭客户端
        client.close();
    }
}
