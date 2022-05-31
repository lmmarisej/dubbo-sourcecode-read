package org.apache.dubbo.remoting.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.googlecode.jsonrpc4j.JsonRpcBasicServer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.webapp.WebAppContext;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * 服务端。
 *
 * @author lmmarise.j@gmail.com
 * @since 2022/5/31 18:57
 */
public class JsonRpcServer {
    private static final ObjectMapper om = new ObjectMapper();

    private UserServiceImpl userService;
    private Class<UserService> userServiceClass;

    public JsonRpcServer(UserServiceImpl userService, Class<UserService> userServiceClass) {
        this.userService = userService;
        this.userServiceClass = userServiceClass;
    }

    public void handle(HttpServletRequest request, HttpServletResponse response) throws IOException {
        new JsonRpcBasicServer(om, userService)
            .handle(request.getInputStream(), response.getOutputStream());
    }

    public static void main(String[] args) throws Throwable {
        // 服务器的监听端口
        Server server = new Server(9999);
        // 关联一个已经存在的上下文
        WebAppContext context = new WebAppContext();
        // 设置描述符位置
        context.setDescriptor("/Users/lmmarise.j/develop/java_web_project_list/dubbo-sourcecode-read/dubbo-remoting/dubbo-remoting-http/src/test/resources/WEB-INF/web.xml");
        // 设置Web内容上下文路径
        context.setResourceBase("/Users/lmmarise.j/develop/java_web_project_list/dubbo-sourcecode-read/dubbo-remoting/dubbo-remoting-http/src/test/resources/webapp");
        // 设置上下文路径
        context.setContextPath("/");
        context.setParentLoaderPriority(true);
        server.setHandler(context);
        server.start();
        server.join();
    }
}
