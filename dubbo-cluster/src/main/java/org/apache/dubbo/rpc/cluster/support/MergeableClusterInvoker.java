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
package org.apache.dubbo.rpc.cluster.support;

import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.ConfigUtils;
import org.apache.dubbo.rpc.AsyncRpcResult;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.cluster.Directory;
import org.apache.dubbo.rpc.cluster.LoadBalance;
import org.apache.dubbo.rpc.cluster.Merger;
import org.apache.dubbo.rpc.cluster.merger.MergerFactory;
import org.apache.dubbo.rpc.model.ApplicationModel;
import org.apache.dubbo.rpc.model.ScopeModelUtil;

import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.dubbo.rpc.Constants.ASYNC_KEY;
import static org.apache.dubbo.rpc.Constants.MERGER_KEY;

/**
 * 对多个 Provider 节点返回结果合并。
 *
 * 如果 merger 参数以 "." 开头，则表示 "." 后的内容是一个方法名，这个方法名是远程目标方法的返回类型中的一个方法，
 * MergeableClusterInvoker 在拿到所有 Invoker 返回的结果对象之后，会遍历每个返回结果，并调用 merger 参数指定的方法，合并这些结果值。
 */
@SuppressWarnings("unchecked")
public class MergeableClusterInvoker<T> extends AbstractClusterInvoker<T> {

    private static final Logger log = LoggerFactory.getLogger(MergeableClusterInvoker.class);

    public MergeableClusterInvoker(Directory<T> directory) {
        super(directory);
    }

    @Override
    protected Result doInvoke(Invocation invocation, List<Invoker<T>> invokers, LoadBalance loadbalance) throws RpcException {
        checkInvokers(invokers, invocation);
        String merger = getUrl().getMethodParameter(invocation.getMethodName(), MERGER_KEY);
        // 判断要调用的目标方法是否有合并器，如果没有，则不会进行合并，找到第一个可用的 Invoker 直接调用并返回结果
        if (ConfigUtils.isEmpty(merger)) { // If a method doesn't have a merger, only invoke one Group
            for (final Invoker<T> invoker : invokers) {
                if (invoker.isAvailable()) {
                    try {
                        return invokeWithContext(invoker, invocation);
                    } catch (RpcException e) {
                        if (e.isNoInvokerAvailableAfterFilter()) {
                            log.debug("No available provider for service" + getUrl().getServiceKey() + " on group "
                                + invoker.getUrl().getGroup() + ", will continue to try another group.");
                        } else {
                            throw e;
                        }
                    }
                }
            }
            return invokeWithContext(invokers.iterator().next(), invocation);
        }

        Class<?> returnType;
        try {
            // 确定目标方法的返回值类型
            returnType = getInterface().getMethod(invocation.getMethodName(), invocation.getParameterTypes()).getReturnType();
        } catch (NoSuchMethodException e) {
            returnType = null;
        }

        Map<String, Result> results = new HashMap<>();
        for (final Invoker<T> invoker : invokers) {  // 调用每个Invoker对象(异步方式)，将请求结果记录到results集合中
            RpcInvocation subInvocation = new RpcInvocation(invocation, invoker);
            subInvocation.setAttachment(ASYNC_KEY, "true");
            results.put(invoker.getUrl().getServiceKey(), invokeWithContext(invoker, subInvocation));
        }

        Object result;

        List<Result> resultList = new ArrayList<>(results.size());

        for (Map.Entry<String, Result> entry : results.entrySet()) {        // 等待结果返回
            Result asyncResult = entry.getValue();
            try {
                Result r = asyncResult.get(Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
                if (r.hasException()) {
                    log.error("Invoke " + getGroupDescFromServiceKey(entry.getKey()) +
                        " failed: " + r.getException().getMessage(), r.getException());
                } else {
                    resultList.add(r);
                }
            } catch (Exception e) {
                throw new RpcException("Failed to invoke service " + entry.getKey() + ": " + e.getMessage(), e);
            }
        }

        if (resultList.isEmpty()) {
            return AsyncRpcResult.newDefaultAsyncResult(invocation);
        } else if (resultList.size() == 1) {
            return AsyncRpcResult.newDefaultAsyncResult(resultList.get(0).getValue(), invocation);
        }

        if (returnType == void.class) {
            return AsyncRpcResult.newDefaultAsyncResult(invocation);
        }

        if (merger.startsWith(".")) {       // merger如果以"."开头，后面为方法名，这个方法名是远程目标方法的返回类型中的方法
            merger = merger.substring(1);
            Method method;
            try {
                method = returnType.getMethod(merger, returnType);
            } catch (NoSuchMethodException | NullPointerException e) {
                throw new RpcException("Can not merge result because missing method [ " + merger + " ] in class [ " +
                    returnType.getName() + " ]");
            }
            if (!Modifier.isPublic(method.getModifiers())) {
                method.setAccessible(true);
            }
            result = resultList.remove(0).getValue();
            try {
                // 得到每个Provider节点返回的结果对象之后，会遍历每个返回对象，调用merger参数指定的方法
                if (method.getReturnType() != void.class
                    && method.getReturnType().isAssignableFrom(result.getClass())) {
                    for (Result r : resultList) {       // resultList集合保存了所有的返回对象，method是Method对象，也就是merger指定的方法
                        result = method.invoke(result, r.getValue());       // result是最后返回调用方的结果
                    }
                } else {
                    for (Result r : resultList) {       // 反射调用
                        method.invoke(result, r.getValue());
                    }
                }
            } catch (Exception e) {
                throw new RpcException("Can not merge result: " + e.getMessage(), e);
            }
        } else {
            Merger resultMerger;
            ApplicationModel applicationModel = ScopeModelUtil.getApplicationModel(invocation.getModuleModel().getApplicationModel());

            if (ConfigUtils.isDefault(merger)) {        // merger参数为true或者default，表示使用默认的Merger扩展实现完成合并
                resultMerger = applicationModel.getBeanFactory().getBean(MergerFactory.class).getMerger(returnType);
            } else {
                // merger 参数指定了 Merger 的扩展名称，则使用 SPI 查找对应的 Merger 扩展实现对象
                resultMerger = applicationModel.getExtensionLoader(Merger.class).getExtension(merger);
            }
            if (resultMerger != null) {
                List<Object> rets = new ArrayList<>(resultList.size());
                for (Result r : resultList) {
                    rets.add(r.getValue());
                }
                // 执行合并操作
                result = resultMerger.merge(rets.toArray((Object[]) Array.newInstance(returnType, 0)));
            } else {
                throw new RpcException("There is no merger to merge result.");
            }
        }
        return AsyncRpcResult.newDefaultAsyncResult(result, invocation);
    }


    @Override
    public Class<T> getInterface() {
        return directory.getInterface();
    }

    @Override
    public boolean isAvailable() {
        return directory.isAvailable();
    }

    @Override
    public void destroy() {
        directory.destroy();
    }

    private String getGroupDescFromServiceKey(String key) {
        int index = key.indexOf("/");
        if (index > 0) {
            return "group [ " + key.substring(0, index) + " ]";
        }
        return key;
    }
}
