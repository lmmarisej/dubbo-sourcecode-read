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
package org.apache.dubbo.rpc.proxy;

import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.ClassUtils;
import org.apache.dubbo.common.utils.ReflectUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.rpc.Constants;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.ProxyFactory;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.model.ServiceModel;
import org.apache.dubbo.rpc.service.Destroyable;
import org.apache.dubbo.rpc.service.EchoService;
import org.apache.dubbo.rpc.service.GenericService;

import java.util.Arrays;
import java.util.LinkedHashSet;

import static org.apache.dubbo.common.constants.CommonConstants.COMMA_SPLIT_PATTERN;
import static org.apache.dubbo.rpc.Constants.INTERFACES;

/**
 * 主要处理的是需要代理的接口。
 */
public abstract class AbstractProxyFactory implements ProxyFactory {
    private static final Class<?>[] INTERNAL_INTERFACES = new Class<?>[]{
        EchoService.class, Destroyable.class
    };

    private static final Logger logger = LoggerFactory.getLogger(AbstractProxyFactory.class);

    @Override
    public <T> T getProxy(Invoker<T> invoker) throws RpcException {
        return getProxy(invoker, false);
    }

    @Override
    public <T> T getProxy(Invoker<T> invoker, boolean generic) throws RpcException {
        // when compiling with native image, ensure that the order of the interfaces remains unchanged
        LinkedHashSet<Class<?>> interfaces = new LinkedHashSet<>();     // 记录要代理的接口
        ClassLoader classLoader = getClassLoader(invoker);

        String config = invoker.getUrl().getParameter(INTERFACES);      // 获取 URL 中 interfaces 参数指定的接口
        if (StringUtils.isNotEmpty(config)) {
            String[] types = COMMA_SPLIT_PATTERN.split(config);         // 按照逗号切分 interfaces 参数，得到接口集合
            for (String type : types) {
                try {
                    interfaces.add(ReflectUtils.forName(classLoader, type));     // 记录这些接口信息
                } catch (Throwable e) {
                    // ignore
                }

            }
        }

        Class<?> realInterfaceClass = null;
        if (generic) {       // 针对泛化接口的处理
            try {
                // find the real interface from url
                String realInterface = invoker.getUrl().getParameter(Constants.INTERFACE);     // 获取 Invoker 中 interface 字段指定的接口
                realInterfaceClass = ReflectUtils.forName(classLoader, realInterface);
                interfaces.add(realInterfaceClass);
            } catch (Throwable e) {
                // ignore
            }

            if (GenericService.class.equals(invoker.getInterface()) || !GenericService.class.isAssignableFrom(invoker.getInterface())) {
                interfaces.add(com.alibaba.dubbo.rpc.service.GenericService.class);
            }
        }

        interfaces.add(invoker.getInterface());     // 获取 Invoker 中指定的接口
        interfaces.addAll(Arrays.asList(INTERNAL_INTERFACES));      // 额外增加接口

        try {
            return getProxy(invoker, interfaces.toArray(new Class<?>[0]));     // 调用抽象的getProxy()重载方法
        } catch (Throwable t) {
            if (generic) {
                if (realInterfaceClass != null) {
                    interfaces.remove(realInterfaceClass);
                }
                interfaces.remove(invoker.getInterface());

                logger.error("Error occur when creating proxy. Invoker is in generic mode. Trying to create proxy without real interface class.", t);
                return getProxy(invoker, interfaces.toArray(new Class<?>[0]));
            } else {
                throw t;
            }
        }
    }

    private <T> ClassLoader getClassLoader(Invoker<T> invoker) {
        ServiceModel serviceModel = invoker.getUrl().getServiceModel();
        ClassLoader classLoader = null;
        if (serviceModel != null && serviceModel.getConfig() != null) {
            classLoader = serviceModel.getConfig().getInterfaceClassLoader();
        }
        if (classLoader == null) {
            classLoader = ClassUtils.getClassLoader();
        }
        return classLoader;
    }

    public static Class<?>[] getInternalInterfaces() {
        return INTERNAL_INTERFACES.clone();
    }

    /**
     * 创建代理对象。
     */
    public abstract <T> T getProxy(Invoker<T> invoker, Class<?>[] types);

}
