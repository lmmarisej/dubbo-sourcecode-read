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
package org.apache.dubbo.rpc.support;

import com.alibaba.fastjson.JSON;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.ExtensionDirector;
import org.apache.dubbo.common.extension.ExtensionInjector;
import org.apache.dubbo.common.utils.*;
import org.apache.dubbo.rpc.*;

import java.lang.reflect.Constructor;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.dubbo.rpc.Constants.*;

/**
 * 解析各类 Mock 配置，并根据不同 Mock 配置进行不同的 Mock 操作。
 */
final public class MockInvoker<T> implements Invoker<T> {
    private final ProxyFactory proxyFactory;
    private final static Map<String, Invoker<?>> MOCK_MAP = new ConcurrentHashMap<>();
    private final static Map<String, Throwable> THROWABLE_MAP = new ConcurrentHashMap<>();

    private final URL url;
    private final Class<T> type;

    public MockInvoker(URL url, Class<T> type) {
        this.url = url;
        this.type = type;
        this.proxyFactory = url.getOrDefaultFrameworkModel().getExtensionLoader(ProxyFactory.class).getAdaptiveExtension();
    }

    public static Object parseMockValue(String mock) throws Exception {
        return parseMockValue(mock, null);
    }

    /**
     * 解析各类 mock 配置
     */
    public static Object parseMockValue(String mock, Type[] returnTypes) throws Exception {
        Object value;
        if ("empty".equals(mock)) {
            value = ReflectUtils.getEmptyObject(returnTypes != null && returnTypes.length > 0 ? (Class<?>) returnTypes[0] : null);
        } else if ("null".equals(mock)) {
            value = null;
        } else if ("true".equals(mock)) {
            value = true;
        } else if ("false".equals(mock)) {
            value = false;
        } else if (mock.length() >= 2 && (mock.startsWith("\"") && mock.endsWith("\"") || mock.startsWith("\'") && mock.endsWith("\'"))) {
            value = mock.subSequence(1, mock.length() - 1);
        } else if (returnTypes != null && returnTypes.length > 0 && returnTypes[0] == String.class) {
            value = mock;
        } else if (StringUtils.isNumeric(mock, false)) {
            value = JSON.parse(mock);
        } else if (mock.startsWith("{")) {
            value = JSON.parseObject(mock, Map.class);
        } else if (mock.startsWith("[")) {
            value = JSON.parseObject(mock, List.class);
        } else {
            value = mock;
        }
        if (ArrayUtils.isNotEmpty(returnTypes)) {
            value = PojoUtils.realize(value, (Class<?>) returnTypes[0], returnTypes.length > 1 ? returnTypes[1] : null);
        }
        return value;
    }

    @Override
    public Result invoke(Invocation invocation) throws RpcException {
        if (invocation instanceof RpcInvocation) {
            ((RpcInvocation) invocation).setInvoker(this);
        }
        // 获取mock值(会从URL中的methodName.mock参数或mock参数获取)
        String mock = getUrl().getMethodParameter(invocation.getMethodName(), MOCK_KEY);

        if (StringUtils.isBlank(mock)) {         // 没有配置 mock 值，直接抛出异常
            throw new RpcException(new IllegalAccessException("mock can not be null. url :" + url));
        }
        mock = normalizeMock(URL.decode(mock));         // mock 值进行处理，去除 "force:"、"fail:" 前缀等
        if (mock.startsWith(RETURN_PREFIX)) {           // mock 值以 return 开头
            mock = mock.substring(RETURN_PREFIX.length()).trim();
            try {
                Type[] returnTypes = RpcUtils.getReturnTypes(invocation);   // 获取响应结果的类型
                Object value = parseMockValue(mock, returnTypes);           // 根据结果类型，对 mock 值中结果值进行转换
                return AsyncRpcResult.newDefaultAsyncResult(value, invocation); // 将固定的mock值设置到Result中
            } catch (Exception ew) {
                throw new RpcException("mock return invoke error. method :" + invocation.getMethodName() + ", mock:" + mock + ", url: " + url, ew);
            }
        } else if (mock.startsWith(THROW_PREFIX)) {     // mock 值以 throw 开头
            mock = mock.substring(THROW_PREFIX.length()).trim();
            if (StringUtils.isBlank(mock)) {            // 未指定异常类型，直接抛出 RpcException
                throw new RpcException("mocked exception for service degradation.");
            } else { // user customized class           // 抛出自定义异常
                Throwable t = getThrowable(mock);
                throw new RpcException(RpcException.BIZ_EXCEPTION, t);
            }
        } else { //impl mock            // 执行 mockService 得到 mock 结果
            try {
                Invoker<T> invoker = getInvoker(mock);
                return invoker.invoke(invocation);
            } catch (Throwable t) {
                throw new RpcException("Failed to create mock implementation class " + mock, t);
            }
        }
    }

    public static Throwable getThrowable(String throwstr) {
        Throwable throwable = THROWABLE_MAP.get(throwstr);
        if (throwable != null) {
            return throwable;
        }

        try {
            Throwable t;
            Class<?> bizException = ReflectUtils.forName(throwstr);
            Constructor<?> constructor;
            constructor = ReflectUtils.findConstructor(bizException, String.class);
            t = (Throwable) constructor.newInstance(new Object[]{"mocked exception for service degradation."});
            if (THROWABLE_MAP.size() < 1000) {
                THROWABLE_MAP.put(throwstr, t);
            }
            return t;
        } catch (Exception e) {
            throw new RpcException("mock throw error :" + throwstr + " argument error.", e);
        }
    }

    /**
     * 处理 MOCK_MAP 缓存的读写、Mock 实现类的查找、生成和调用 Invoker
     */
    @SuppressWarnings("unchecked")
    private Invoker<T> getInvoker(String mock) {
        Class<T> serviceType = (Class<T>) ReflectUtils.forName(url.getServiceInterface());      // 根据serviceType查找mock的实现类
        String mockService = ConfigUtils.isDefault(mock) ? serviceType.getName() + "Mock" : mock;
        Invoker<T> invoker = (Invoker<T>) MOCK_MAP.get(mockService);        // 尝试从MOCK_MAP集合中获取对应的Invoker对象
        if (invoker != null) {
            return invoker;
        }

        // 创建Invoker对象
        T mockObject = (T) getMockObject(url.getOrDefaultApplicationModel().getExtensionDirector(), mock, serviceType);
        invoker = proxyFactory.getInvoker(mockObject, serviceType, url);        // 创建Invoker对象
        if (MOCK_MAP.size() < 10000) {
            MOCK_MAP.put(mockService, invoker);
        }
        return invoker;
    }

    /**
     * 检查 mockService 参数是否为 true 或 default，如果是的话，则在服务接口后添加 Mock 字符串，作为服务接口的 Mock 实现；
     * 如果不是的话，则直接将 mockService 实现作为服务接口的 Mock 实现。
     */
    @SuppressWarnings("unchecked")
    public static Object getMockObject(ExtensionDirector extensionDirector, String mockService, Class serviceType) {
        boolean isDefault = ConfigUtils.isDefault(mockService);
        if (isDefault) {
            // 如果 mock 为 true 或 default 值，会在服务接口后添加 Mock 字符串，得到对应的实现类名称，并进行实例化
            mockService = serviceType.getName() + "Mock";
        }

        Class<?> mockClass;
        try {
            mockClass = ReflectUtils.forName(mockService);
        } catch (Exception e) {
            if (!isDefault) {// does not check Spring bean if it is default config.
                ExtensionInjector extensionFactory = extensionDirector.getExtensionLoader(ExtensionInjector.class).getAdaptiveExtension();
                Object obj = extensionFactory.getInstance(serviceType, mockService);
                if (obj != null) {
                    return obj;
                }
            }
            throw new IllegalStateException("Did not find mock class or instance " + mockService + ", please check if there's mock class or instance implementing interface " + serviceType.getName(), e);
        }
        if (mockClass == null || !serviceType.isAssignableFrom(mockClass)) {         // 检查 mockClass 是否继承 serviceType 接口
            throw new IllegalStateException("The mock class " + mockClass.getName() + " not implement interface " + serviceType.getName());
        }

        try {
            return mockClass.newInstance();
        } catch (InstantiationException e) {
            throw new IllegalStateException("No default constructor from mock class " + mockClass.getName(), e);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException(e);
        }
    }


    /**
     * Normalize mock string:
     *
     * <ol>
     * <li>return => return null</li>
     * <li>fail => default</li>
     * <li>force => default</li>
     * <li>fail:throw/return foo => throw/return foo</li>
     * <li>force:throw/return foo => throw/return foo</li>
     * </ol>
     *
     * @param mock mock string
     * @return normalized mock string
     */
    public static String normalizeMock(String mock) {
        if (mock == null) {
            return mock;
        }

        mock = mock.trim();

        if (mock.length() == 0) {
            return mock;
        }

        if (RETURN_KEY.equalsIgnoreCase(mock)) {
            return RETURN_PREFIX + "null";
        }

        if (ConfigUtils.isDefault(mock) || "fail".equalsIgnoreCase(mock) || "force".equalsIgnoreCase(mock)) {
            return "default";
        }

        if (mock.startsWith(FAIL_PREFIX)) {
            mock = mock.substring(FAIL_PREFIX.length()).trim();
        }

        if (mock.startsWith(FORCE_PREFIX)) {
            mock = mock.substring(FORCE_PREFIX.length()).trim();
        }

        if (mock.startsWith(RETURN_PREFIX) || mock.startsWith(THROW_PREFIX)) {
            mock = mock.replace('`', '"');
        }

        return mock;
    }

    @Override
    public URL getUrl() {
        return this.url;
    }

    @Override
    public boolean isAvailable() {
        return true;
    }

    @Override
    public void destroy() {
        //do nothing
    }

    @Override
    public Class<T> getInterface() {
        return type;
    }
}
