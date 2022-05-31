package org.apache.dubbo.remoting.http;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * @author lmmarise.j@gmail.com
 * @since 2022/5/31 18:57
 */
public class RpcServlet extends HttpServlet {
    private JsonRpcServer rpcServer = null;

    public RpcServlet() {
        super();
        // JsonRpcServer会按照json-rpc请求，调用UserServiceImpl中的方法
        rpcServer = new JsonRpcServer(new UserServiceImpl(), UserService.class);
    }

    @Override
    protected void service(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        rpcServer.handle(request, response);
    }
}
