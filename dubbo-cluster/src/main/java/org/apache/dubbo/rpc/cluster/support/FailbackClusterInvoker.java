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
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.timer.HashedWheelTimer;
import org.apache.dubbo.common.timer.Timeout;
import org.apache.dubbo.common.timer.Timer;
import org.apache.dubbo.common.timer.TimerTask;
import org.apache.dubbo.common.utils.NamedThreadFactory;
import org.apache.dubbo.rpc.AsyncRpcResult;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.Directory;
import org.apache.dubbo.rpc.cluster.LoadBalance;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_FAILBACK_TIMES;
import static org.apache.dubbo.common.constants.CommonConstants.RETRIES_KEY;
import static org.apache.dubbo.rpc.cluster.Constants.DEFAULT_FAILBACK_TASKS;
import static org.apache.dubbo.rpc.cluster.Constants.FAIL_BACK_TASKS_KEY;

/**
 * FailbackClusterInvoker 在请求失败之后，返回一个空结果给 Consumer，同时还会添加一个定时任务对失败的请求进行重试。
 *
 * When fails, record failure requests and schedule for retry on a regular interval.
 * Especially useful for services of notification.
 *
 * <a href="http://en.wikipedia.org/wiki/Failback">Failback</a>
 */
public class FailbackClusterInvoker<T> extends AbstractClusterInvoker<T> {

    private static final Logger logger = LoggerFactory.getLogger(FailbackClusterInvoker.class);

    private static final long RETRY_FAILED_PERIOD = 5;

    /**
     * Number of retries obtained from the configuration, don't contain the first invoke.
     */
    private final int retries;
    private final int failbackTasks;
    private volatile Timer failTimer;

    public FailbackClusterInvoker(Directory<T> directory) {
        super(directory);

        int retriesConfig = getUrl().getParameter(RETRIES_KEY, DEFAULT_FAILBACK_TIMES);
        if (retriesConfig < 0) {
            retriesConfig = DEFAULT_FAILBACK_TIMES;
        }
        int failbackTasksConfig = getUrl().getParameter(FAIL_BACK_TASKS_KEY, DEFAULT_FAILBACK_TASKS);
        if (failbackTasksConfig <= 0) {
            failbackTasksConfig = DEFAULT_FAILBACK_TASKS;
        }
        retries = retriesConfig;
        failbackTasks = failbackTasksConfig;
    }

    private void addFailed(LoadBalance loadbalance, Invocation invocation, List<Invoker<T>> invokers, Invoker<T> lastInvoker, URL consumerUrl) {
        if (failTimer == null) {
            synchronized (this) {
                if (failTimer == null) {        // Double Check 防止并发问题
                    failTimer = new HashedWheelTimer(       // 初始化时间轮，这个时间轮有 32 个槽，每个槽代表 1 秒
                        new NamedThreadFactory("failback-cluster-timer", true),
                        1,
                        TimeUnit.SECONDS, 32, failbackTasks);
                }
            }
        }
        // 创建一个定时任务
        RetryTimerTask retryTimerTask = new RetryTimerTask(loadbalance, invocation, invokers, lastInvoker, retries, RETRY_FAILED_PERIOD, consumerUrl);
        try {
            failTimer.newTimeout(retryTimerTask, RETRY_FAILED_PERIOD, TimeUnit.SECONDS);   // 将定时任务添加到时间轮中
        } catch (Throwable e) {
            logger.error("Failback background works error, invocation->" + invocation + ", exception: " + e.getMessage());
        }
    }

    @Override
    protected Result doInvoke(Invocation invocation, List<Invoker<T>> invokers, LoadBalance loadbalance) throws RpcException {
        Invoker<T> invoker = null;
        URL consumerUrl = RpcContext.getServiceContext().getConsumerUrl();
        try {
            checkInvokers(invokers, invocation);         // 检测 Invoker 集合是否为空
            invoker = select(loadbalance, invocation, invokers, null);    // 调用 select() 方法得到此次尝试的 Invoker 对象
            // Asynchronous call method must be used here, because failback will retry in the background.
            // Then the serviceContext will be cleared after the call is completed.
            return invokeWithContextAsync(invoker, invocation, consumerUrl);      // 调用 invoke() 方法完成远程调用
        } catch (Throwable e) {
            logger.error("Failback to invoke method " + invocation.getMethodName() +
                ", wait for retry in background. Ignored exception: " + e.getMessage() + ", ", e);
            if (retries > 0) {
                addFailed(loadbalance, invocation, invokers, invoker, consumerUrl); // 请求失败之后，会添加一个定时任务进行重试
            }
            return AsyncRpcResult.newDefaultAsyncResult(null, null, invocation); // 请求失败时，会返回一个空结果
        }
    }

    @Override
    public void destroy() {
        super.destroy();
        if (failTimer != null) {
            failTimer.stop();
        }
    }

    /**
     * 重新调用 select() 方法筛选合适的 Invoker 对象，并尝试进行请求。
     */
    private class RetryTimerTask implements TimerTask {
        private final Invocation invocation;
        private final LoadBalance loadbalance;
        private final List<Invoker<T>> invokers;
        private final long tick;
        private Invoker<T> lastInvoker;
        private URL consumerUrl;

        /**
         * Number of retries obtained from the configuration, don't contain the first invoke.
         */
        private final int retries;

        /**
         * Number of retried.
         */
        private int retriedTimes = 0;

        RetryTimerTask(LoadBalance loadbalance, Invocation invocation, List<Invoker<T>> invokers, Invoker<T> lastInvoker,
                       int retries, long tick, URL consumerUrl) {
            this.loadbalance = loadbalance;
            this.invocation = invocation;
            this.invokers = invokers;
            this.retries = retries;
            this.tick = tick;
            this.lastInvoker = lastInvoker;
            this.consumerUrl = consumerUrl;
        }

        @Override
        public void run(Timeout timeout) {
            try {
                logger.info("Attempt to retry to invoke method " + invocation.getMethodName() +
                        ". The total will retry " + retries + " times, the current is the " + retriedTimes + " retry");
                // 重新选择 Invoker 对象，注意，这里会将上次重试失败的 Invoker 作为 selected 集合传入
                Invoker<T> retryInvoker = select(loadbalance, invocation, invokers, Collections.singletonList(lastInvoker));
                lastInvoker = retryInvoker;
                invokeWithContextAsync(retryInvoker, invocation, consumerUrl);   // 请求对应的Provider节点
            } catch (Throwable e) {
                logger.error("Failed retry to invoke method " + invocation.getMethodName() + ", waiting again.", e);
                if ((++retriedTimes) >= retries) {      // 重试次数达到上限，输出警告日志
                    logger.error("Failed retry times exceed threshold (" + retries + "), We have to abandon, invocation->" + invocation);
                } else {
                    rePut(timeout);     // 重试次数未达到上限，则重新添加定时任务，等待重试
                }
            }
        }

        private void rePut(Timeout timeout) {
            if (timeout == null) {      // 边界检查
                return;
            }

            Timer timer = timeout.timer();
            if (timer.isStop() || timeout.isCancelled()) {      // 检查时间轮状态、检查定时任务状态
                return;
            }

            timer.newTimeout(timeout.task(), tick, TimeUnit.SECONDS);  // 重新添加定时任务
        }
    }
}
