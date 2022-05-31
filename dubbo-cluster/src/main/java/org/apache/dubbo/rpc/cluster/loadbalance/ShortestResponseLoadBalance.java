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
package org.apache.dubbo.rpc.cluster.loadbalance;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.threadpool.manager.FrameworkExecutorRepository;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcStatus;
import org.apache.dubbo.rpc.cluster.Constants;
import org.apache.dubbo.rpc.model.ApplicationModel;
import org.apache.dubbo.rpc.model.ScopeModelAware;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 最短响应时间的负载均衡算法.
 * <p>
 * 从多个 Provider 节点中选出调用成功的且响应时间最短的 Provider 节点，
 * 不过满足该条件的 Provider 节点可能有多个，所以还要再使用随机算法进行一次选择，得到最终要调用的 Provider 节点。
 * </p>
 * Filter the number of invokers with the shortest response time of
 * success calls and count the weights and quantities of these invokers in last slide window.
 * If there is only one invoker, use the invoker directly;
 * if there are multiple invokers and the weights are not the same, then random according to the total weight;
 * if there are multiple invokers and the same weight, then randomly called.
 */
public class ShortestResponseLoadBalance extends AbstractLoadBalance implements ScopeModelAware {

    public static final String NAME = "shortestresponse";

    private int slidePeriod = 30_000;

    private ConcurrentMap<RpcStatus, SlideWindowData> methodMap = new ConcurrentHashMap<>();

    private AtomicBoolean onResetSlideWindow = new AtomicBoolean(false);

    private volatile long lastUpdateTime = System.currentTimeMillis();

    private ExecutorService executorService;

    @Override
    public void setApplicationModel(ApplicationModel applicationModel) {
        slidePeriod = applicationModel.getModelEnvironment().getConfiguration().getInt(Constants.SHORTEST_RESPONSE_SLIDE_PERIOD, 30_000);
        executorService = applicationModel.getFrameworkModel().getBeanFactory()
            .getBean(FrameworkExecutorRepository.class).getSharedExecutor();
    }

    protected static class SlideWindowData {

        private long succeededOffset;
        private long succeededElapsedOffset;
        private RpcStatus rpcStatus;

        public SlideWindowData(RpcStatus rpcStatus) {
            this.rpcStatus = rpcStatus;
            this.succeededOffset = 0;
            this.succeededElapsedOffset = 0;
        }

        public void reset() {
            this.succeededOffset = rpcStatus.getSucceeded();
            this.succeededElapsedOffset = rpcStatus.getSucceededElapsed();
        }

        /**
         * 获取调用成功的平均时间，具体计算方式是：调用成功的请求数总数对应的总耗时 / 调用成功的请求数总数 = 成功调用的平均时间
         */
        private long getSucceededAverageElapsed() {
            long succeed = this.rpcStatus.getSucceeded() - this.succeededOffset;
            if (succeed == 0) {
                return 0;
            }
            return (this.rpcStatus.getSucceededElapsed() - this.succeededElapsedOffset) / succeed;
        }

        public long getEstimateResponse() {
            int active = this.rpcStatus.getActive() + 1;
            return getSucceededAverageElapsed() * active;
        }
    }

    @Override
    protected <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation) {
        // Number of invokers
        int length = invokers.size();          // 记录Invoker集合的数量
        // Estimated shortest response time of all invokers
        long shortestResponse = Long.MAX_VALUE;        // 用于记录所有Invoker集合中最短响应时间
        // The number of invokers having the same estimated shortest response time
        int shortestCount = 0;          // 具有相同最短响应时间的Invoker个数
        // The index of invokers having the same estimated shortest response time
        int[] shortestIndexes = new int[length];           // 存放所有最短响应时间的Invoker的下标
        // the weight of every invokers
        int[] weights = new int[length];           // 存储每个Invoker的权重
        // The sum of the warmup weights of all the shortest response  invokers
        int totalWeight = 0;            // 存储权重总和
        // The weight of the first shortest response invokers
        int firstWeight = 0;                // 记录第一个Invoker对象的权重
        // Every shortest response invoker has the same weight value?
        boolean sameWeight = true;          // 最短响应时间Invoker集合中的Invoker权重是否相同

        // Filter out all the shortest response invokers
        for (int i = 0; i < length; i++) {
            Invoker<T> invoker = invokers.get(i);
            RpcStatus rpcStatus = RpcStatus.getStatus(invoker.getUrl(), invocation.getMethodName());
            SlideWindowData slideWindowData = methodMap.computeIfAbsent(rpcStatus, SlideWindowData::new);

            // Calculate the estimated response time from the product of active connections and succeeded average elapsed time.
            // 计算一个处理新请求的预估值，也就是如果当前请求发给这个Provider，大概耗时多久处理完成
            long estimateResponse = slideWindowData.getEstimateResponse();
            int afterWarmup = getWeight(invoker, invocation);             // 计算该Invoker的权重（主要是处理预热）
            weights[i] = afterWarmup;
            // Same as LeastActiveLoadBalance
            if (estimateResponse < shortestResponse) {
                shortestResponse = estimateResponse;            // 第一次找到Invoker集合中最短响应耗时的Invoker对象，记录其相关信息
                shortestCount = 1;
                shortestIndexes[0] = i;
                totalWeight = afterWarmup;
                firstWeight = afterWarmup;
                sameWeight = true;
            } else if (estimateResponse == shortestResponse) {
                shortestIndexes[shortestCount++] = i;          // 出现多个耗时最短的Invoker对象
                totalWeight += afterWarmup;
                if (sameWeight && i > 0
                    && afterWarmup != firstWeight) {
                    sameWeight = false;
                }
            }
        }

        if (System.currentTimeMillis() - lastUpdateTime > slidePeriod
            && onResetSlideWindow.compareAndSet(false, true)) {
            //reset slideWindowData in async way
            executorService.execute(() -> {
                methodMap.values().forEach(SlideWindowData::reset);
                lastUpdateTime = System.currentTimeMillis();
                onResetSlideWindow.set(false);
            });
        }

        if (shortestCount == 1) {
            return invokers.get(shortestIndexes[0]);
        }
        // 如果耗时最短的所有Invoker对象的权重不相同，则通过加权随机负载均衡的方式选择一个Invoker返回
        if (!sameWeight && totalWeight > 0) {
            int offsetWeight = ThreadLocalRandom.current().nextInt(totalWeight);
            for (int i = 0; i < shortestCount; i++) {
                int shortestIndex = shortestIndexes[i];
                offsetWeight -= weights[shortestIndex];
                if (offsetWeight < 0) {
                    return invokers.get(shortestIndex);
                }
            }
        }
        // 如果耗时最短的所有Invoker对象的权重相同，则随机返回一个
        return invokers.get(shortestIndexes[ThreadLocalRandom.current().nextInt(shortestCount)]);
    }
}
