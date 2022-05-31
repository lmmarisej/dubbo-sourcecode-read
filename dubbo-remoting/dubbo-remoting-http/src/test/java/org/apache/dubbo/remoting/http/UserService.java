package org.apache.dubbo.remoting.http;

/**
 * 分别用来创建 User、查询 User 以及相关信息、删除 User
 *
 * @author lmmarise.j@gmail.com
 * @since 2022/5/31 18:47
 */
public interface UserService {
    User createUser(int userId, String name, int age);

    User getUser(int userId);

    String getUserName(int userId);

    int getUserId(String name);

    void deleteAll();
}
