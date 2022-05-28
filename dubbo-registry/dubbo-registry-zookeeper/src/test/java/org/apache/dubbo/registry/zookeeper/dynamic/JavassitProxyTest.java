package org.apache.dubbo.registry.zookeeper.dynamic;

import javassist.util.proxy.MethodHandler;
import javassist.util.proxy.ProxyFactory;

import java.lang.reflect.Method;

/**
 * @author lmmarise.j@gmail.com
 * @since 2022/5/28 14:31
 */
public class JavassitProxyTest {
    static class JavassistDemo {
        private String prop = "MyName";

        public void setProp(String var1) {
            this.prop = var1;
        }

        public String getProp() {
            return this.prop;
        }

        public JavassistDemo() {
            this.prop = "MyName";
        }

        public void execute() {
            System.out.println("execute():" + this.prop);
        }
    }

    public static void main(String[] args) throws Exception {
        ProxyFactory factory = new ProxyFactory();
        factory.setSuperclass(JavassistDemo.class);
        factory.setFilter(m -> m.getName().equals("execute"));      // 指定拦截方法
        // 设置拦截处理
        factory.setHandler(new MethodHandler() {
            @Override
            public Object invoke(Object self, Method thisMethod, Method proceed, Object[] args) throws Throwable {
                System.out.println("前置处理");
                Object result = proceed.invoke(self, args);
                System.out.println("执行结果:" + result);
                System.out.println("后置处理");
                return result;
            }
        });
        // 创建JavassistDemo的代理类，并创建代理对象
        Class<?> c = factory.createClass();
        JavassistDemo JavassistDemo = (JavassistDemo) c.newInstance();
        JavassistDemo.execute(); // 执行execute()方法，会被拦截
        System.out.println(JavassistDemo.getProp());
    }
}
