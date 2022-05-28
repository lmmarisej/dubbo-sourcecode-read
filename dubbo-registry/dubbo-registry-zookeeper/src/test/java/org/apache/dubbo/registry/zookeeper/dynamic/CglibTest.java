package org.apache.dubbo.registry.zookeeper.dynamic;

import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;

import java.lang.reflect.Method;

/**
 * @author lmmarise.j@gmail.com
 * @since 2022/5/28 13:27
 */
public class CglibTest {
    public String method(String str) {
        System.out.println("原生方法：" + str);
        return str;
    }

    public static void main(String[] args) {
        CglibProxy proxy = new CglibProxy();
        CglibTest proxyImp = (CglibTest) proxy.getProxy(CglibTest.class);
        String result = proxyImp.method("test");
        System.out.println(result);
    }
}

class CglibProxy implements MethodInterceptor {
    private final Enhancer enhancer = new Enhancer();

    public Object getProxy(Class clazz) {
        enhancer.setSuperclass(clazz);      // 指定被代理的类
        enhancer.setCallback(this);         // 对根据当前代理类的的方法的调用，都将被路由到 MethodInterceptor#intercept
        return enhancer.create();           // 根据 Class 与 MethodInterceptor 信息，通过 ASM 生成代理实例
    }

    /**
     * 拦截对父类方法的调用。
     *
     * @param obj    enhancer#create 生成的代理实例
     * @param method 被调用的原方法
     * @param args   调用者传入的方法参数
     * @param proxy  包含子类和父类的被调用的方法签名，以及子类父类的类型
     * @return 代理方法返回值
     */
    @Override
    public Object intercept(Object obj, Method method, Object[] args, MethodProxy proxy) throws Throwable {
        System.out.println("前置处理");
        Object result = proxy.invokeSuper(obj, args);   // 调用父类中的方法
        System.out.println("后置处理");
        return result;
    }
}
