package org.apache.dubbo.registry.zookeeper;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;

import java.util.List;

/**
 * @author lmmarise.j@gmail.com
 * @since 2022/5/28 02:45
 */
public class CuratorWatcherTest {
    public static void main(String[] args) throws Exception {
        String zkAddress = "127.0.0.1:2181";
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.newClient(zkAddress, retryPolicy);
        client.start();
        try {
            client.create().withMode(CreateMode.PERSISTENT).forPath("/user", "test".getBytes());
        } catch (Exception e) {
        }
        // 这里通过 usingWatcher() 方法给 Children 添加一个 Watcher，Children 节点变化才会触发，/user 节点变化不会
        List<String> children = client.getChildren().usingWatcher(new CuratorWatcher() {
            public void process(WatchedEvent event) {
                System.out.println(event.getType() + "," + event.getPath());
            }
        }).forPath("/user");
        System.out.println(children);
        System.in.read();
    }
}
