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
package org.apache.dubbo.rpc;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.Adaptive;
import org.apache.dubbo.common.extension.SPI;

import static org.apache.dubbo.common.extension.ExtensionScope.FRAMEWORK;
import static org.apache.dubbo.rpc.Constants.PROXY_KEY;

/**
 * 创建代理对象的工厂，提供了创建代理的能力。
 *
 * proxy 个包中支持 JDK 动态代理以及 Javassist 字节码两种方式生成本地代理类。
 *
 * ProxyFactory. (API/SPI, Singleton, ThreadSafe)
 */
@SPI(value = "javassist", scope = FRAMEWORK)        // 默认实现使用 Javassist 来创建代码对象。
public interface ProxyFactory {

    /**
     * 为传入的 Invoker 对象创建代理对象
     */
    @Adaptive({PROXY_KEY})
    <T> T getProxy(Invoker<T> invoker) throws RpcException;

    @Adaptive({PROXY_KEY})
    <T> T getProxy(Invoker<T> invoker, boolean generic) throws RpcException;

    /**
     * 将传入的代理对象封装成 Invoker 对象，可以暂时理解为 getProxy() 的逆操作。
     */
    @Adaptive({PROXY_KEY})
    <T> Invoker<T> getInvoker(T proxy, Class<T> type, URL url) throws RpcException;

}
