package org.apache.dubbo.registry.zookeeper;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.time.LocalDateTime;
import java.util.List;

/**
 * @author lmmarise.j@gmail.com
 * @since 2022/5/28 02:55
 */
public class CuratorCacheTest {
    public static void main(String[] args) throws Exception {
        String zkAddress = "127.0.0.1:2181";
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.newClient(zkAddress, retryPolicy);
        client.start();
        NodeCache nodeCache = new NodeCache(client, "/user");
        // start()方法有个boolean类型的参数，默认是false。如果设置为true， 那么NodeCache在第一次启动的时候就会立刻从ZooKeeper上读取对应节点的数据内容，并保存在Cache中。
        nodeCache.start(true);
        if (nodeCache.getCurrentData() != null) {
            System.out.println("NodeCache节点初始化数据为：" + new String(nodeCache.getCurrentData().getData()));
        } else {
            System.out.println("NodeCache节点数据为空");
        }

        // 添加监听器
        nodeCache.getListenable().addListener(() -> {
            String data = new String(nodeCache.getCurrentData().getData());
            System.out.println("NodeCache节点路径：" + nodeCache.getCurrentData().getPath() + "，节点数据为：" + data);
        });

        // 创建PathChildrenCache实例，监听的是"user"这个节点
        PathChildrenCache childrenCache = new PathChildrenCache(client, "/user", true);
        // StartMode指定的初始化的模式
        // NORMAL:普通异步初始化
        // BUILD_INITIAL_CACHE:同步初始化
        // POST_INITIALIZED_EVENT:异步初始化，初始化之后会触发事件
        childrenCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
        // childrenCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
        // childrenCache.start(PathChildrenCache.StartMode.NORMAL);
        List<ChildData> children = childrenCache.getCurrentData();
        System.out.println("获取子节点列表：");
        // 如果是BUILD_INITIAL_CACHE可以获取这个数据，如果不是就不行
        children.forEach(childData -> {
            System.out.println(new String(childData.getData()));
        });
        childrenCache.getListenable().addListener(((client1, event) -> {
            System.out.println(LocalDateTime.now() + "  " + event.getType());
            if (event.getType().equals(PathChildrenCacheEvent.Type.INITIALIZED)) {
                System.out.println("PathChildrenCache:子节点初始化成功...");
            } else if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_ADDED)) {
                String path = event.getData().getPath();
                System.out.println("PathChildrenCache添加子节点:" + event.getData().getPath());
                System.out.println("PathChildrenCache子节点数据:" + new String(event.getData().getData()));
            } else if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_REMOVED)) {
                System.out.println("PathChildrenCache删除子节点:" + event.getData().getPath());
            } else if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_UPDATED)) {
                System.out.println("PathChildrenCache修改子节点路径:" + event.getData().getPath());
                System.out.println("PathChildrenCache修改子节点数据:" + new String(event.getData().getData()));
            }
        }));

        // 创建TreeCache实例监听"user"节点
        TreeCache cache = TreeCache.newBuilder(client, "/user").setCacheData(false).build();
        cache.getListenable().addListener((c, event) -> {
            if (event.getData() != null) {
                System.out.println("TreeCache,type=" + event.getType() + " path=" + event.getData().getPath());
            } else {
                System.out.println("TreeCache,type=" + event.getType());
            }
        });
        cache.start();
        System.in.read();
    }
}
