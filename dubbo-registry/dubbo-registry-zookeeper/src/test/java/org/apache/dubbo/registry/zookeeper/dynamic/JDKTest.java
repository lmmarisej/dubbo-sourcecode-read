package org.apache.dubbo.registry.zookeeper.dynamic;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

/**
 * @author lmmarise.j@gmail.com
 * @since 2022/5/28 12:52
 */
public class JDKTest {
    public static void main(String[] args) {
        Subject subject = new RealSubject();
        DemoInvokerHandler invokerHandler = new DemoInvokerHandler(subject);
        Subject proxy = (Subject) invokerHandler.getProxy();
        proxy.operation();
    }
}

class DemoInvokerHandler implements InvocationHandler {
    private final Object target;

    public DemoInvokerHandler(Object target) {
        this.target = target;
    }

    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        System.out.println("JDK proxy before...");
        Object result = method.invoke(target, args);
        System.out.println("JDK proxy after...");
        return result;
    }

    public Object getProxy() {
        return Proxy.newProxyInstance(
            Thread.currentThread().getContextClassLoader(),
            target.getClass().getInterfaces(),
            this
        );
    }
}
