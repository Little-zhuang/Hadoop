package com.zhuang.zookeeperAPI;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.Test;

/**
 * @author Elke
 * @Description 节点操作的watch机制
 * @date 2021/2/20-14:13
 */
public class WatchNode {

    @Test
    public void watchNode() throws Exception{
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(3000,3);

        String connect = "192.168.91.100:2181,192.168.91.110:2181,192.168.91.120:2181";
        CuratorFramework client = CuratorFrameworkFactory.newClient(connect,1000,1000,retryPolicy);

        client.start();

        //创建一个treeCache对象，指定要监听的节点路劲
        TreeCache treeCache = new TreeCache(client,"/hello2");

        //自定义一个监听器
        treeCache.getListenable().addListener(new TreeCacheListener() {
            @Override
            public void childEvent(CuratorFramework curatorFramework, TreeCacheEvent treeCacheEvent) throws Exception {
                ChildData data = treeCacheEvent.getData();
                if (data != null){
                    switch (treeCacheEvent.getType()){
                        case NODE_ADDED:
                            System.out.println("监控到有新增节点！");
                            break;
                        case NODE_REMOVED:
                            System.out.println("监控到节点被移除！");
                            break;
                        case NODE_UPDATED:
                            System.out.println("监控到节点被更新！");
                            break;
                        default:
                            break;
                    }
                }
            }
        });
        //开始监听
        treeCache.start();

        Thread.sleep(10000000);
    }
}
