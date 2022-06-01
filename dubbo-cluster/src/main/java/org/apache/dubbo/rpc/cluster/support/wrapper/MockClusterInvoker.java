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
package org.apache.dubbo.rpc.cluster.support.wrapper;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.ConfigUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.rpc.AsyncRpcResult;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.cluster.ClusterInvoker;
import org.apache.dubbo.rpc.cluster.Directory;
import org.apache.dubbo.rpc.support.MockInvoker;

import java.util.List;

import static org.apache.dubbo.rpc.Constants.MOCK_KEY;
import static org.apache.dubbo.rpc.cluster.Constants.FORCE_KEY;
import static org.apache.dubbo.rpc.cluster.Constants.INVOCATION_NEED_MOCK;

/**
 * MockClusterInvoker 是 Dubbo Mock 机制的核心。
 *
 * 根据配置决定请求是否启动了 Mock 机制以及在何种情况下才会触发 Mock。
 */
public class MockClusterInvoker<T> implements ClusterInvoker<T> {

    private static final Logger logger = LoggerFactory.getLogger(MockClusterInvoker.class);

    private final Directory<T> directory;
    private final Invoker<T> invoker;

    public MockClusterInvoker(Directory<T> directory, Invoker<T> invoker) {
        this.directory = directory;
        this.invoker = invoker;
    }

    @Override
    public URL getUrl() {
        return directory.getConsumerUrl();
    }

    @Override
    public URL getRegistryUrl() {
        return directory.getUrl();
    }

    @Override
    public Directory<T> getDirectory() {
        return directory;
    }

    @Override
    public boolean isDestroyed() {
        return directory.isDestroyed();
    }

    @Override
    public boolean isAvailable() {
        return directory.isAvailable();
    }

    @Override
    public void destroy() {
        this.invoker.destroy();
    }

    @Override
    public Class<T> getInterface() {
        return directory.getInterface();
    }

    /**
     * 判断是否需要开启 Mock 机制。
     *
     * 在 mock 参数中配置的是 force 模式，则会直接调用 doMockInvoke() 方法进行 mock。
     * 如果在 mock 参数中配置的是 fail 模式，则会正常调用 Invoker 发起请求，在请求失败的时候，会调动 doMockInvoke() 方法进行 mock。
     */
    @Override
    public Result invoke(Invocation invocation) throws RpcException {
        Result result;

        // 从 URL 中获取方法对应的 mock 配置
        String value = getUrl().getMethodParameter(invocation.getMethodName(), MOCK_KEY, Boolean.FALSE.toString()).trim();
        if (ConfigUtils.isEmpty(value)) {
            // 若 mock 参数未配置或是配置为 false，则不会开启 Mock 机制，直接调用底层的 Invoker
            result = this.invoker.invoke(invocation);
        } else if (value.startsWith(FORCE_KEY)) {
            if (logger.isWarnEnabled()) {
                logger.warn("force-mock: " + invocation.getMethodName() + " force-mock enabled , url : " + getUrl());
            }
            //force:direct mock
            // 若 mock 参数配置为 force，则表示强制 mock，直接调用 doMockInvoke() 方法
            result = doMockInvoke(invocation, null);
        } else {
            //fail-mock
            // 如果 mock 配置的不是 force，那配置的就是 fail，会继续调用 Invoker 对象的 invoke() 方法进行请求
            try {
                result = this.invoker.invoke(invocation);

                //fix:#4585
                if (result.getException() != null && result.getException() instanceof RpcException) {
                    RpcException rpcException = (RpcException) result.getException();
                    if (rpcException.isBiz()) {     // 如果是业务异常，会直接抛出
                        throw rpcException;
                    } else {
                        result = doMockInvoke(invocation, rpcException);  // 如果是非业务异常，会调用 doMockInvoke() 方法返回 mock 结果
                    }
                }

            } catch (RpcException e) {
                if (e.isBiz()) {
                    throw e;
                }

                if (logger.isWarnEnabled()) {
                    logger.warn("fail-mock: " + invocation.getMethodName() + " fail-mock enabled , url : " + getUrl(), e);
                }
                result = doMockInvoke(invocation, e);
            }
        }
        return result;
    }

    /**
     * 获取 MockInvoker 对象，并调用其 invoke() 方法进行 mock 操作。
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private Result doMockInvoke(Invocation invocation, RpcException e) {
        Result result;
        Invoker<T> mockInvoker;

        // 调用 selectMockInvoker() 方法过滤得到 MockInvoker
        List<Invoker<T>> mockInvokers = selectMockInvoker(invocation);
        if (CollectionUtils.isEmpty(mockInvokers)) {
            // 如果 selectMockInvoker() 方法未返回 MockInvoker 对象，则创建一个 MockInvoker
            mockInvoker = (Invoker<T>) new MockInvoker(getUrl(), directory.getInterface());
        } else {
            mockInvoker = mockInvokers.get(0);
        }
        try {
            result = mockInvoker.invoke(invocation);     // 调用 MockInvoker.invoke() 方法进行 mock
        } catch (RpcException mockException) {
            if (mockException.isBiz()) {         // 如果是业务异常，则在Result中设置该异常
                result = AsyncRpcResult.newDefaultAsyncResult(mockException.getCause(), invocation);
            } else {
                throw new RpcException(mockException.getCode(), getMockExceptionMessage(e, mockException), mockException.getCause());
            }
        } catch (Throwable me) {
            throw new RpcException(getMockExceptionMessage(e, me), me.getCause());
        }
        return result;
    }

    private String getMockExceptionMessage(Throwable t, Throwable mt) {
        String msg = "mock error : " + mt.getMessage();
        if (t != null) {
            msg = msg + ", invoke error is :" + StringUtils.toString(t);
        }
        return msg;
    }

    /**
     * 将 Invocation 附属信息中的 invocation.need.mock 属性设置为 true，然后交给 Directory 中的 Router 集合进行处理。
     *
     * Return MockInvoker
     * Contract：
     * directory.list() will return a list of normal invokers if Constants.INVOCATION_NEED_MOCK is absent or not true in invocation, otherwise, a list of mock invokers will return.
     * if directory.list() returns more than one mock invoker, only one of them will be used.
     *
     * @param invocation
     * @return
     */
    private List<Invoker<T>> selectMockInvoker(Invocation invocation) {
        List<Invoker<T>> invokers = null;
        //TODO generic invoker？
        if (invocation instanceof RpcInvocation) {
            //Note the implicit contract (although the description is added to the interface declaration, but extensibility is a problem. The practice placed in the attachment needs to be improved)
            invocation.setAttachment(INVOCATION_NEED_MOCK, Boolean.TRUE.toString());
            //directory will return a list of normal invokers if Constants.INVOCATION_NEED_MOCK is absent or not true in invocation, otherwise, a list of mock invokers will return.
            try {
                // 将 Invocation 附属信息中的 invocation.need.mock 属性设置为 true
                RpcContext.getServiceContext().setConsumerUrl(getUrl());
                invokers = directory.list(invocation);
            } catch (RpcException e) {
                if (logger.isInfoEnabled()) {
                    logger.info("Exception when try to invoke mock. Get mock invokers error for service:"
                            + getUrl().getServiceInterface() + ", method:" + invocation.getMethodName()
                            + ", will construct a new mock with 'new MockInvoker()'.", e);
                }
            }
        }
        return invokers;
    }

    @Override
    public String toString() {
        return "invoker :" + this.invoker + ",directory: " + this.directory;
    }
}
