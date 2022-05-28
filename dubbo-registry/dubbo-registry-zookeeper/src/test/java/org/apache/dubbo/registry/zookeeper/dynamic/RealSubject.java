package org.apache.dubbo.registry.zookeeper.dynamic;

/**
 * @author lmmarise.j@gmail.com
 * @since 2022/5/28 13:27
 */
class RealSubject implements Subject {

    @Override
    public void operation() {
        System.out.println("I'm RealSubject#operation.");
    }
}
