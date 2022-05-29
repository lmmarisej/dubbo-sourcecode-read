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

import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.threadpool.ThreadlessExecutor;
import org.apache.dubbo.rpc.model.ConsumerMethodModel;
import org.apache.dubbo.rpc.protocol.dubbo.FutureAdapter;

import java.util.Map;
import java.util.concurrent.*;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static org.apache.dubbo.common.constants.CommonConstants.PROVIDER_ASYNC_KEY;
import static org.apache.dubbo.common.utils.ReflectUtils.defaultReturn;

/**
 * 表示的是一个异步的、未完成的 RPC 调用，其中会记录对应 RPC 调用的信息。
 * <p>
 * This class represents an unfinished RPC call, it will hold some context information for this call, for example RpcContext and Invocation,
 * so that when the call finishes and the result returns, it can guarantee all the contexts being recovered as the same as when the call was made
 * before any callback is invoked.
 * <p>
 * TODO if it's reasonable or even right to keep a reference to Invocation?
 * <p>
 * As {@link Result} implements CompletionStage, {@link AsyncRpcResult} allows you to easily build a async filter chain whose status will be
 * driven entirely by the state of the underlying RPC call.
 * <p>
 * AsyncRpcResult does not contain any concrete value (except the underlying value bring by CompletableFuture), consider it as a status transfer node.
 * {@link #getValue()} and {@link #getException()} are all inherited from {@link Result} interface, implementing them are mainly
 * for compatibility consideration. Because many legacy {@link Filter} implementation are most possibly to call getValue directly.
 */
public class AsyncRpcResult implements Result {
    private static final Logger logger = LoggerFactory.getLogger(AsyncRpcResult.class);

    /**
     * 真正执行 AsyncRpcResult 上添加的回调方法的线程可能先后处理过多个不同的 AsyncRpcResult，所以我们需要传递并保存当前的 RpcContext。
     *
     * RpcContext may already have been changed when callback happens, it happens when the same thread is used to execute another RPC call.
     * So we should keep the copy of current RpcContext instance and restore it before callback being executed.
     */
    private RpcContext.RestoreContext storedContext;        // 用于存储相关的 RpcContext 对象。

    private Executor executor;      // 此次 RPC 调用关联的线程池。

    private Invocation invocation;      // 此次 RPC 调用关联的 Invocation 对象。
    private final boolean async;

    private CompletableFuture<AppResponse> responseFuture;  // 是 DefaultFuture 回调链上的一个 Future

    /**
     * Whether set future to Thread Local when invocation mode is sync
     */
    private static final boolean setFutureWhenSync = Boolean.parseBoolean(System.getProperty(CommonConstants.SET_FUTURE_IN_SYNC_MODE, "true"));

    /**
     * 除了接收发送请求返回的 CompletableFuture<AppResponse> 对象，还会将当前的 RpcContext 保存到 storedContext 和 storedServerContext 中
     */
    public AsyncRpcResult(CompletableFuture<AppResponse> future, Invocation invocation) {
        this.responseFuture = future;
        this.invocation = invocation;
        RpcInvocation rpcInvocation = (RpcInvocation) invocation;
        if ((rpcInvocation.get(PROVIDER_ASYNC_KEY) != null || InvokeMode.SYNC != rpcInvocation.getInvokeMode()) && !future.isDone()) {
            async = true;
            this.storedContext = RpcContext.clearAndStoreContext();
        } else {
            async = false;
        }
    }

    /**
     * Notice the return type of {@link #getValue} is the actual type of the RPC method, not {@link AppResponse}
     *
     * @return
     */
    @Override
    public Object getValue() {
        return getAppResponse().getValue();
    }

    /**
     * CompletableFuture can only be completed once, so try to update the result of one completed CompletableFuture will
     * have no effect. To avoid this problem, we check the complete status of this future before update its value.
     * <p>
     * But notice that trying to give an uncompleted CompletableFuture a new specified value may face a race condition,
     * because the background thread watching the real result will also change the status of this CompletableFuture.
     * The result is you may lose the value you expected to set.
     *
     * @param value
     */
    @Override
    public void setValue(Object value) {
        try {
            if (responseFuture.isDone()) {
                responseFuture.get().setValue(value);
            } else {
                AppResponse appResponse = new AppResponse(invocation);
                appResponse.setValue(value);
                responseFuture.complete(appResponse);
            }
        } catch (Exception e) {
            // This should not happen in normal request process;
            logger.error("Got exception when trying to fetch the underlying result from AsyncRpcResult.");
            throw new RpcException(e);
        }
    }

    @Override
    public Throwable getException() {
        return getAppResponse().getException();
    }

    @Override
    public void setException(Throwable t) {
        try {
            if (responseFuture.isDone()) {
                responseFuture.get().setException(t);
            } else {
                AppResponse appResponse = new AppResponse(invocation);
                appResponse.setException(t);
                responseFuture.complete(appResponse);
            }
        } catch (Exception e) {
            // This should not happen in normal request process;
            logger.error("Got exception when trying to fetch the underlying result from AsyncRpcResult.");
            throw new RpcException(e);
        }
    }

    @Override
    public boolean hasException() {
        return getAppResponse().hasException();
    }

    public CompletableFuture<AppResponse> getResponseFuture() {
        return responseFuture;
    }

    public void setResponseFuture(CompletableFuture<AppResponse> responseFuture) {
        this.responseFuture = responseFuture;
    }

    /**
     * 从 responseFuture 中拿到 AppResponse 对象
     */
    public Result getAppResponse() {
        try {
            if (responseFuture.isDone()) {      // 检测responseFuture是否已完成
                return responseFuture.get();    // 获取AppResponse
            }
        } catch (Exception e) {
            // This should not happen in normal request process;
            logger.error("Got exception when trying to fetch the underlying result from AsyncRpcResult.");
            throw new RpcException(e);
        }

        return createDefaultValue(invocation);       // 根据调用方法的返回值，生成默认值
    }

    /**
     * This method will always return after a maximum 'timeout' waiting:
     * 1. if value returns before timeout, return normally.
     * 2. if no value returns after timeout, throw TimeoutException.
     *
     * @return
     * @throws InterruptedException
     * @throws ExecutionException
     */
    @Override
    public Result get() throws InterruptedException, ExecutionException {
        // 针对ThreadlessExecutor的特殊处理，这里调用waitAndDrain()等待响应
        if (executor != null && executor instanceof ThreadlessExecutor) {
            ThreadlessExecutor threadlessExecutor = (ThreadlessExecutor) executor;
            threadlessExecutor.waitAndDrain();      // 阻塞等待响应返回
        }
        // 非ThreadlessExecutor线程池的场景中，则直接调用Future(最底层是DefaultFuture)的get()方法阻塞
        return responseFuture.get();
    }

    @Override
    public Result get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        if (executor != null && executor instanceof ThreadlessExecutor) {
            ThreadlessExecutor threadlessExecutor = (ThreadlessExecutor) executor;
            threadlessExecutor.waitAndDrain();
        }
        return responseFuture.get(timeout, unit);
    }

    @Override
    public Object recreate() throws Throwable {
        RpcInvocation rpcInvocation = (RpcInvocation) invocation;
        if (InvokeMode.FUTURE == rpcInvocation.getInvokeMode()) {
            return RpcContext.getClientAttachment().getFuture();
        } else if (InvokeMode.ASYNC == rpcInvocation.getInvokeMode()) {
            return createDefaultValue(invocation).recreate();
        }

        return getAppResponse().recreate();
    }

    /**
     * 可以为 AsyncRpcResult 添加回调方法，而这个回调方法会被包装一层并注册到 responseFuture 上
     */
    public Result whenCompleteWithContext(BiConsumer<Result, Throwable> fn) {
        this.responseFuture = this.responseFuture.whenComplete((v, t) -> {    // 在 responseFuture 之上注册回调
            if (async) {
                RpcContext.restoreContext(storedContext);       // 将当前线程的 RpcContext 引用到拷贝到另一个线程上
            }
            fn.accept(v, t);
        });

        if (setFutureWhenSync || ((RpcInvocation) invocation).getInvokeMode() != InvokeMode.SYNC) {
            // Necessary! update future in context, see https://github.com/apache/dubbo/issues/9461
            RpcContext.getServiceContext().setFuture(new FutureAdapter<>(this.responseFuture));
        }

        return this;
    }

    @Override
    public <U> CompletableFuture<U> thenApply(Function<Result, ? extends U> fn) {
        return this.responseFuture.thenApply(fn);
    }

    @Override
    @Deprecated
    public Map<String, String> getAttachments() {
        return getAppResponse().getAttachments();
    }

    @Override
    public Map<String, Object> getObjectAttachments() {
        return getAppResponse().getObjectAttachments();
    }

    @Override
    public void setAttachments(Map<String, String> map) {
        getAppResponse().setAttachments(map);
    }

    @Override
    public void setObjectAttachments(Map<String, Object> map) {
        getAppResponse().setObjectAttachments(map);
    }

    @Deprecated
    @Override
    public void addAttachments(Map<String, String> map) {
        getAppResponse().addAttachments(map);
    }

    @Override
    public void addObjectAttachments(Map<String, Object> map) {
        getAppResponse().addObjectAttachments(map);
    }

    @Override
    public String getAttachment(String key) {
        return getAppResponse().getAttachment(key);
    }

    @Override
    public Object getObjectAttachment(String key) {
        return getAppResponse().getObjectAttachment(key);
    }

    @Override
    public String getAttachment(String key, String defaultValue) {
        return getAppResponse().getAttachment(key, defaultValue);
    }

    @Override
    public Object getObjectAttachment(String key, Object defaultValue) {
        return getAppResponse().getObjectAttachment(key, defaultValue);
    }

    @Override
    public void setAttachment(String key, String value) {
        setObjectAttachment(key, value);
    }

    @Override
    public void setAttachment(String key, Object value) {
        setObjectAttachment(key, value);
    }

    @Override
    public void setObjectAttachment(String key, Object value) {
        getAppResponse().setAttachment(key, value);
    }

    public Executor getExecutor() {
        return executor;
    }

    public void setExecutor(Executor executor) {
        this.executor = executor;
    }

    /**
     * Some utility methods used to quickly generate default AsyncRpcResult instance.
     */
    public static AsyncRpcResult newDefaultAsyncResult(AppResponse appResponse, Invocation invocation) {
        return new AsyncRpcResult(CompletableFuture.completedFuture(appResponse), invocation);
    }

    public static AsyncRpcResult newDefaultAsyncResult(Invocation invocation) {
        return newDefaultAsyncResult(null, null, invocation);
    }

    public static AsyncRpcResult newDefaultAsyncResult(Object value, Invocation invocation) {
        return newDefaultAsyncResult(value, null, invocation);
    }

    public static AsyncRpcResult newDefaultAsyncResult(Throwable t, Invocation invocation) {
        return newDefaultAsyncResult(null, t, invocation);
    }

    public static AsyncRpcResult newDefaultAsyncResult(Object value, Throwable t, Invocation invocation) {
        CompletableFuture<AppResponse> future = new CompletableFuture<>();
        AppResponse result = new AppResponse(invocation);
        if (t != null) {
            result.setException(t);
        } else {
            result.setValue(value);
        }
        future.complete(result);
        return new AsyncRpcResult(future, invocation);
    }

    private static Result createDefaultValue(Invocation invocation) {
        ConsumerMethodModel method = (ConsumerMethodModel) invocation.get(Constants.METHOD_MODEL);
        return method != null ? new AppResponse(defaultReturn(method.getReturnClass())) : new AppResponse();
    }
}

