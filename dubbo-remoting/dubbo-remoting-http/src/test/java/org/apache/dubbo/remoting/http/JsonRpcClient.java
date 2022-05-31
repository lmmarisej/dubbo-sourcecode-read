package org.apache.dubbo.remoting.http;

import com.googlecode.jsonrpc4j.JsonRpcHttpClient;

import java.net.URL;

/**
 * 客户端。
 *
 * @author lmmarise.j@gmail.com
 * @since 2022/5/31 18:58
 */
public class JsonRpcClient {
    private static JsonRpcHttpClient rpcHttpClient;

    public static void main(String[] args) throws Throwable {
        // 创建 JsonRpcHttpClient
        rpcHttpClient = new JsonRpcHttpClient(new URL("http://127.0.0.1:9999/rpc"));
        JsonRpcClient jsonRpcClient = new JsonRpcClient();
        jsonRpcClient.deleteAll(); // 调用 deleteAll() 方法删除全部User
        // 调用 createUser() 方法创建 User
        System.out.println(jsonRpcClient.createUser(1, "testName", 30));
        // 调用getUser()、getUserName()、getUserId()方法进行查询
        System.out.println(jsonRpcClient.getUser(1));
        System.out.println(jsonRpcClient.getUserName(1));
        System.out.println(jsonRpcClient.getUserId("testName"));
    }

    public void deleteAll() throws Throwable {
        // 调用服务端的deleteAll()方法
        rpcHttpClient.invoke("deleteAll", null);
    }

    public User createUser(int userId, String name, int age) throws Throwable {
        Object[] params = new Object[]{userId, name, age};
        // 调用服务端的createUser()方法
        return rpcHttpClient.invoke("createUser", params, User.class);
    }

    public User getUser(int userId) throws Throwable {
        Integer[] params = new Integer[]{userId};
        // 调用服务端的getUser()方法
        return rpcHttpClient.invoke("getUser", params, User.class);
    }

    public String getUserName(int userId) throws Throwable {
        Integer[] params = new Integer[]{userId};
        // 调用服务端的getUserName()方法
        return rpcHttpClient.invoke("getUserName", params, String.class);
    }

    public int getUserId(String name) throws Throwable {
        String[] params = new String[]{name};
        // 调用服务端的getUserId()方法
        return rpcHttpClient.invoke("getUserId", params, Integer.class);
    }
}
