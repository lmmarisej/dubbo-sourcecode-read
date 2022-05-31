package org.apache.dubbo.remoting.http;

import lombok.Data;

import java.io.Serializable;

/**
 * 服务端和客户端都需要的 domain 类以及服务接口。
 *
 * @author lmmarise.j@gmail.com
 * @since 2022/5/31 18:46
 */
@Data
public class User implements Serializable {
    private int userId;
    private String name;
    private int age;
}
