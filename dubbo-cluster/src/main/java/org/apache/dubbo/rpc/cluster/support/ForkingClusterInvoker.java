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

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.threadlocal.NamedInternalThreadFactory;
import org.apache.dubbo.common.threadpool.manager.FrameworkExecutorRepository;
import org.apache.dubbo.rpc.*;
import org.apache.dubbo.rpc.cluster.Directory;
import org.apache.dubbo.rpc.cluster.LoadBalance;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.dubbo.common.constants.CommonConstants.*;
import static org.apache.dubbo.rpc.cluster.Constants.DEFAULT_FORKS;

/**
 * 并发调用多个 Provider 节点，只要有一个 Provider 节点成功返回了结果，ForkingClusterInvoker 的 doInvoke() 方法就会立即结束运行。
 * <p>
 * NOTICE! This implementation does not work well with async call.
 * <p>
 * ForkingClusterInvoker 主要是为了应对一些实时性要求较高的读操作，因为没有并发控制的多线程写入，可能会导致数据不一致。
 *
 * <p>
 * Invoke a specific number of invokers concurrently, usually used for demanding real-time operations, but need to waste more service resources.
 *
 * <a href="http://en.wikipedia.org/wiki/Fork_(topology)">Fork</a>
 */
public class ForkingClusterInvoker<T> extends AbstractClusterInvoker<T> {

    /**
     * Use {@link NamedInternalThreadFactory} to produce {@link org.apache.dubbo.common.threadlocal.InternalThread}
     * which with the use of {@link org.apache.dubbo.common.threadlocal.InternalThreadLocal} in {@link RpcContext}.
     */
    private final ExecutorService executor;     // 维护一个线程池

    public ForkingClusterInvoker(Directory<T> directory) {
        super(directory);
        executor = directory.getUrl().getOrDefaultFrameworkModel().getBeanFactory()
            .getBean(FrameworkExecutorRepository.class).getSharedExecutor();
    }

    /**
     * 从 Invoker 集合中选出指定个数（forks 参数决定）的 Invoker 对象，然后通过 executor 线程池并发调用这些 Invoker，
     * 并将请求结果存储在 ref 阻塞队列中，则当前线程会阻塞在 ref 队列上，等待第一个请求结果返回。
     */
    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public Result doInvoke(final Invocation invocation, List<Invoker<T>> invokers, LoadBalance loadbalance) throws RpcException {
        try {
            checkInvokers(invokers, invocation);         // 检查Invoker集合是否为空
            final List<Invoker<T>> selected;
            final int forks = getUrl().getParameter(FORKS_KEY, DEFAULT_FORKS);    // 从 URL 中获取 forks 参数，作为并发请求的上限，默认值为 2
            final int timeout = getUrl().getParameter(TIMEOUT_KEY, DEFAULT_TIMEOUT);
            if (forks <= 0 || forks >= invokers.size()) {
                selected = invokers;         // 如果 forks 为负数或是大于 Invoker 集合的长度，会直接并发调用全部 Invoker
            } else {
                selected = new ArrayList<>(forks);        // 按照forks指定的并发度，选择此次并发调用的Invoker对象
                while (selected.size() < forks) {
                    Invoker<T> invoker = select(loadbalance, invocation, invokers, selected);
                    if (!selected.contains(invoker)) {
                        // Avoid add the same invoker several times.
                        selected.add(invoker);       // 避免重复选择
                    }
                }
            }
            RpcContext.getServiceContext().setInvokers((List) selected);
            final AtomicInteger count = new AtomicInteger();                // 记录失败的请求个数
            final BlockingQueue<Object> ref = new LinkedBlockingQueue<>(1);     // 用于记录请求结果
            for (final Invoker<T> invoker : selected) {     // 遍历 selected 列表
                URL consumerUrl = RpcContext.getServiceContext().getConsumerUrl();
                executor.execute(() -> {        // 为每个Invoker创建一个任务，并提交到线程池中
                    try {
                        if (ref.size() > 0) {
                            return;
                        }
                        Result result = invokeWithContextAsync(invoker, invocation, consumerUrl);       // 发起请求
                        ref.offer(result);        // 将请求结果写到 ref 队列中
                    } catch (Throwable e) {
                        int value = count.incrementAndGet();
                        if (value >= selected.size()) {
                            ref.offer(e);         // 如果失败的请求个数超过了并发请求的个数，则向 ref 队列中写入异常
                        }
                    }
                });
            }
            try {
                Object ret = ref.poll(timeout, TimeUnit.MILLISECONDS);       // 当前线程会阻塞等待任意一个请求结果的出现
                if (ret instanceof Throwable) {      // 如果结果类型为Throwable，则抛出异常
                    Throwable e = (Throwable) ret;
                    throw new RpcException(e instanceof RpcException ? ((RpcException) e).getCode() : RpcException.UNKNOWN_EXCEPTION,
                        "Failed to forking invoke provider " + selected + ", but no luck to perform the invocation. " +
                            "Last error is: " + e.getMessage(), e.getCause() != null ? e.getCause() : e);
                }
                return (Result) ret;        // 返回结果
            } catch (InterruptedException e) {
                throw new RpcException("Failed to forking invoke provider " + selected + ", " +
                    "but no luck to perform the invocation. Last error is: " + e.getMessage(), e);
            }
        } finally {
            // clear attachments which is binding to current thread.
            RpcContext.getClientAttachment().clearAttachments();           // 清除上下文信息
        }
    }
}
