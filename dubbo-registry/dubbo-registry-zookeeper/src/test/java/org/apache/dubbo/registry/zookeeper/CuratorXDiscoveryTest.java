//package org.apache.dubbo.registry.zookeeper;
//
//import lombok.Data;
//import org.apache.curator.framework.CuratorFramework;
//import org.apache.curator.framework.CuratorFrameworkFactory;
//import org.apache.curator.retry.ExponentialBackoffRetry;
//import org.apache.curator.x.discovery.ServiceCache;
//import org.apache.curator.x.discovery.ServiceDiscovery;
//import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
//import org.apache.curator.x.discovery.ServiceInstance;
//import org.apache.curator.x.discovery.details.InstanceSerializer;
//import org.apache.curator.x.discovery.details.JsonInstanceSerializer;
//
//import java.util.ArrayList;
//import java.util.List;
//import java.util.UUID;
//
///**
// * @author lmmarise.j@gmail.com
// * @since 2022/5/28 03:01
// */
//public class CuratorXDiscoveryTest {
//    private ServiceDiscovery<ServerInfo> serviceDiscovery;
//    private ServiceCache<ServerInfo> serviceCache;
//    private CuratorFramework client;
//    private String root;
//    private InstanceSerializer serializer = new JsonInstanceSerializer<>(ServerInfo.class);
//
//    @Data
//    static class Config {
//        private String path;
//        private String hostPort;
//    }
//
//    @Data
//    static class ServerInfo {
//        private String host;
//        private int port;
//    }
//
//    CuratorXDiscoveryTest(Config config) throws Exception {
//        this.root = config.getPath();
//        client = CuratorFrameworkFactory.newClient(config.getHostPort(), new ExponentialBackoffRetry(1, 1));
//        client.start(); // 启动Curator客户端
//        client.blockUntilConnected();  // 阻塞当前线程，等待连接成功
//        serviceDiscovery = ServiceDiscoveryBuilder   // 创建ServiceDiscovery
//            .builder(ServerInfo.class)
//            .client(client) // 依赖Curator客户端
//            .basePath(root) // 管理的Zk路径
//            .watchInstances(true) // 当ServiceInstance加载
//            .serializer(serializer)
//            .build();
//        serviceDiscovery.start(); // 启动ServiceDiscovery
//        // 创建ServiceCache，监Zookeeper相应节点的变化，也方便后续的读取
//        serviceCache = serviceDiscovery.serviceCacheBuilder().name(root).build();
//        serviceCache.start(); // 启动ServiceCache
//    }
//
//    public void registerRemote(ServerInfo serverInfo) throws Exception {
//        // 将ServerInfo对象转换成ServiceInstance对象
//        ServiceInstance<ServerInfo> thisInstance = ServiceInstance.<ServerInfo>builder()
//            .name(root)
//            .id(UUID.randomUUID().toString()) // 随机生成的UUID
//            .address(serverInfo.getHost()) // host
//            .port(serverInfo.getPort()) // port
//            .payload(serverInfo) // payload
//            .build();
//        // 将ServiceInstance写入到Zookeeper中
//        serviceDiscovery.registerService(thisInstance);
//    }
//
//    public List<ServerInfo> queryRemoteNodes() {
//        List<ServerInfo> ServerInfoDetails = new ArrayList<>();
//        // 查询 ServiceCache 获取全部的 ServiceInstance 对象
//        List<ServiceInstance<ServerInfo>> serviceInstances = serviceCache.getInstances();
//        serviceInstances.forEach(serviceInstance -> {
//            // 从每个ServiceInstance对象的playload字段中反序列化得
//            // 到ServerInfo实例
//            ServerInfo instance = serviceInstance.getPayload();
//            ServerInfoDetails.add(instance);
//        });
//        return ServerInfoDetails;
//    }
//}
