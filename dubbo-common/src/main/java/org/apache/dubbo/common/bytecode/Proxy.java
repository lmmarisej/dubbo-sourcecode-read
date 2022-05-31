/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.common.bytecode;

import org.apache.dubbo.common.utils.ReflectUtils;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.security.ProtectionDomain;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.dubbo.common.constants.CommonConstants.MAX_PROXY_COUNT;

/**
 * 代理相关的工具，同时也代理需要被代理的类。
 */
public class Proxy {
    public static final InvocationHandler THROW_UNSUPPORTED_INVOKER = new InvocationHandler() {
        @Override
        public Object invoke(Object proxy, Method method, Object[] args) {
            throw new UnsupportedOperationException("Method [" + ReflectUtils.getName(method) + "] unimplemented.");
        }
    };

    private static final AtomicLong PROXY_CLASS_COUNTER = new AtomicLong(0);
    // Key 是 ClassLoader 对象，第二层 Key 是上面整理得到的接口拼接而成的，Value 是被缓存的代理类的 WeakReference（弱引用）。
    private static final Map<ClassLoader, Map<String, Proxy>> PROXY_CACHE_MAP = new WeakHashMap<>();    // 类加载器被GC 扫描到时回收

    private final Class<?> classToCreate;

    protected Proxy(Class<?> classToCreate) {
        this.classToCreate = classToCreate;
    }

    /**
     * 提供了动态创建代理类的核心实现。
     *
     * @param ics interface class array.
     * @return Proxy instance.
     */
    public static Proxy getProxy(Class<?>... ics) {
        if (ics.length > MAX_PROXY_COUNT) {
            throw new IllegalArgumentException("interface limit exceeded");
        }

        // ClassLoader from App Interface should support load some class from Dubbo
        ClassLoader cl = ics[0].getClassLoader();
        ProtectionDomain domain = ics[0].getProtectionDomain();

        // use interface class name list as key.
        String key = buildInterfacesKey(cl, ics);        // 接口列表将会作为第二层集合的 Key

        // get cache by class loader.
        final Map<String, Proxy> cache;
        synchronized (PROXY_CACHE_MAP) {     // 加锁同步
            cache = PROXY_CACHE_MAP.computeIfAbsent(cl, k -> new ConcurrentHashMap<>());
        }

        Proxy proxy = cache.get(key);        // 查找到缓存的代理类
        if (proxy == null) {
            synchronized (ics[0]) {
                proxy = cache.get(key);
                if (proxy == null) {
                    // create Proxy class.
                    proxy = new Proxy(buildProxyClass(cl, ics, domain));
                    cache.put(key, proxy);      // 代理接口们名字作为 key，缓存被代理的代理
                }
            }
        }
        return proxy;
    }

    private static String buildInterfacesKey(ClassLoader cl, Class<?>[] ics) {
        StringBuilder sb = new StringBuilder();
        for (Class<?> ic : ics) {
            String itf = ic.getName();
            if (!ic.isInterface()) {        // 传入的必须是接口类，否则直接报错
                throw new RuntimeException(itf + " is not a interface.");
            }

            Class<?> tmp = null;
            try {
                tmp = Class.forName(itf, false, cl);       // 加载接口类，加载失败则直接报错
            } catch (ClassNotFoundException ignore) {
            }

            if (tmp != ic) {
                throw new IllegalArgumentException(ic + " is not visible from class loader");
            }

            sb.append(itf).append(';');     // 将接口类的完整名称用分号连接起来
        }
        return sb.toString();
    }

    private static Class<?> buildProxyClass(ClassLoader cl, Class<?>[] ics, ProtectionDomain domain) {
        ClassGenerator ccp = null;
        try {
            ccp = ClassGenerator.newInstance(cl);  // 代理类的生成

            Set<String> worked = new HashSet<>();
            List<Method> methods = new ArrayList<>();

            String pkg = ics[0].getPackage().getName();
            Class<?> neighbor = ics[0];

            for (Class<?> ic : ics) {
                String npkg = ic.getPackage().getName();
                if (!Modifier.isPublic(ic.getModifiers())) {
                    if (!pkg.equals(npkg)) {        // 如果接口不是public的，则需要保证所有接口在一个包下
                        throw new IllegalArgumentException("non-public interfaces from different packages");
                    }
                }

                ccp.addInterface(ic);       // 向 ClassGenerator 中添加接口

                for (Method method : ic.getMethods()) {     // 遍历接口中的每个方法
                    String desc = ReflectUtils.getDesc(method);
                    if (worked.contains(desc) || Modifier.isStatic(method.getModifiers())) {   // 跳过已经重复方法以及 static 方法
                        continue;
                    }
                    worked.add(desc);   // 将方法描述添加到 worked 这个 Set 集合中，进行去重

                    int ix = methods.size();
                    Class<?> rt = method.getReturnType();           // 获取方法的返回值
                    Class<?>[] pts = method.getParameterTypes();    // 获取方法的参数列表

                    // 创建方法体
                    StringBuilder code = new StringBuilder("Object[] args = new Object[").append(pts.length).append("];");
                    for (int j = 0; j < pts.length; j++) {
                        code.append(" args[").append(j).append("] = ($w)$").append(j + 1).append(";");
                    }
                    code.append(" Object ret = handler.invoke(this, methods[").append(ix).append("], args);");
                    if (!Void.TYPE.equals(rt)) {
                        code.append(" return ").append(asArgument(rt, "ret")).append(';');
                    }

                    methods.add(method);        // 将生成好的方法缓存到 ClassGenerator，通过反射，见下
                    ccp.addMethod(method.getName(), method.getModifiers(), rt, pts, method.getExceptionTypes(), code.toString());
                }
            }

            // create ProxyInstance class.      创建代理实例类（ProxyInstance）和代理类。
            // 向 ClassGenerator 中添加相应的信息，例如，类名、默认构造方法、字段、父类以及一个 newInstance() 方法
            String pcn = neighbor.getName() + "DubboProxy" + PROXY_CLASS_COUNTER.getAndIncrement();
            ccp.setClassName(pcn);      // 全限定名
            ccp.addField("public static java.lang.reflect.Method[] methods;");  // 生成的 methods 集合
            ccp.addField("private " + InvocationHandler.class.getName() + " handler;"); // InvocationHandler 对象
            // 添加构造方法
            ccp.addConstructor(Modifier.PUBLIC, new Class<?>[]{InvocationHandler.class}, new Class<?>[0], "handler=$1;");   // 第一个参数指向 handler
            ccp.addDefaultConstructor();    // 默认构造方法
            Class<?> clazz = ccp.toClass(neighbor, cl, domain);
            clazz.getField("methods").set(null, methods.toArray(new Method[0]));
            return clazz;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        } finally {
            // release ClassGenerator
            if (ccp != null) {
                ccp.release();      // 释放 ClassGenerator 的相关资源
            }
        }
    }

    private static String asArgument(Class<?> cl, String name) {
        if (cl.isPrimitive()) {
            if (Boolean.TYPE == cl) {
                return name + "==null?false:((Boolean)" + name + ").booleanValue()";
            }
            if (Byte.TYPE == cl) {
                return name + "==null?(byte)0:((Byte)" + name + ").byteValue()";
            }
            if (Character.TYPE == cl) {
                return name + "==null?(char)0:((Character)" + name + ").charValue()";
            }
            if (Double.TYPE == cl) {
                return name + "==null?(double)0:((Double)" + name + ").doubleValue()";
            }
            if (Float.TYPE == cl) {
                return name + "==null?(float)0:((Float)" + name + ").floatValue()";
            }
            if (Integer.TYPE == cl) {
                return name + "==null?(int)0:((Integer)" + name + ").intValue()";
            }
            if (Long.TYPE == cl) {
                return name + "==null?(long)0:((Long)" + name + ").longValue()";
            }
            if (Short.TYPE == cl) {
                return name + "==null?(short)0:((Short)" + name + ").shortValue()";
            }
            throw new RuntimeException(name + " is unknown primitive type.");
        }
        return "(" + ReflectUtils.getName(cl) + ")" + name;
    }

    /**
     * get instance with default handler.
     *
     * @return instance.
     */
    public Object newInstance() {
        return newInstance(THROW_UNSUPPORTED_INVOKER);
    }

    /**
     * InvokerInvocationHandler 中维护了一个 Invoker 对象，也是前面 getProxy() 方法传入的第一个参数，
     * 这个 Invoker 不是一个简单的 DubboInvoker 对象，而是在 DubboInvoker 之上经过一系列装饰器修饰的 Invoker 对象。
     *
     * @param handler       无论是 Javassist 还是 JDK 生成的代理类，都会将方法委托给 InvokerInvocationHandler 进行处理。
     */
    public Object newInstance(InvocationHandler handler) {
        Constructor<?> constructor;
        try {
            constructor = classToCreate.getDeclaredConstructor(InvocationHandler.class);
            return constructor.newInstance(handler);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    public Class<?> getClassToCreate() {
        return classToCreate;
    }
}
