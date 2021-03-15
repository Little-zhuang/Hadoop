package com.zhuang.zookeeperAPI;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.Test;

/**
 * @author Elke
 * @Description 获取内容
 * @date 2021/2/17-16:57
 */
public class GetDate {
@Test
    public void getDate() throws Exception{
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000,1);
        String connection = "192.168.91.120:2181";
        CuratorFramework client = CuratorFrameworkFactory.newClient(connection,1000,1000,retryPolicy);
        client.start();
        byte[] bytes = client.getData().forPath("/hello");

        System.out.println(new String(bytes));
        client.close();
    }
}
