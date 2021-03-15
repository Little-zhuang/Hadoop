package com.zhuang.zookeeperAPI;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.Test;

/**
 * @author Elke
 * @Description 修改数据内容
 * @date 2021/2/17-16:38
 */
public class SetDate {

    @Test
    public void setDate() throws Exception{
        //1. 定制重试策略
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000,1);

        //2，获取一个客户端对象
        String connect = "192.168.91.100:2181,192.168.91.110:2181,192.168.91.120:2181";
        CuratorFramework client = CuratorFrameworkFactory.newClient(connect,1000,1000,retryPolicy);

        //3，打开客户端
        client.start();

        //4，修改内容
        client.setData().forPath("/hello","zookeeper".getBytes());
        //5，关闭客户端
        client.close();
    }
}
