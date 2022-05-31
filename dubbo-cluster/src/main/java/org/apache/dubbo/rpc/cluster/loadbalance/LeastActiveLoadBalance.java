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
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcStatus;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 最小活跃数负载均衡算法。
 * <p>
 * 当前活跃请求数越小的 Provider 节点，剩余的处理能力越多，处理请求的效率也就越高，那么该 Provider 在单位时间内就可以处理更多的请求，
 * 所以我们应该优先将请求分配给该 Provider 节点。
 * <p>
 * LeastActiveLoadBalance 需要配合 ActiveLimitFilter 使用，ActiveLimitFilter 会记录每个接口方法的活跃请求数，
 * 在 LeastActiveLoadBalance 进行负载均衡时，只会从活跃请求数最少的 Invoker 集合里挑选 Invoker。
 * <p>
 * Filter the number of invokers with the least number of active calls and count the weights and quantities of these invokers.
 * If there is only one invoker, use the invoker directly;
 * if there are multiple invokers and the weights are not the same, then random according to the total weight;
 * if there are multiple invokers and the same weight, then randomly called.
 */
public class LeastActiveLoadBalance extends AbstractLoadBalance {

    public static final String NAME = "leastactive";

    @Override
    protected <T> Invoker<T> doSelect(List<Invoker<T>> invokers, URL url, Invocation invocation) {
        // Number of invokers
        int length = invokers.size();       // 初始化Invoker数量
        // The least active value of all invokers
        int leastActive = -1;          // 记录最小的活跃请求数
        // The number of invokers having the same least active value (leastActive)
        int leastCount = 0;         // 记录活跃请求数最小的Invoker集合的个数
        // The index of invokers having the same least active value (leastActive)
        int[] leastIndexes = new int[length];           // 记录活跃请求数最小的Invoker在invokers数组中的下标位置
        // the weight of every invokers
        int[] weights = new int[length];            // 记录活跃请求数最小的Invoker集合中，每个Invoker的权重值
        // The sum of the warmup weights of all the least active invokers
        int totalWeight = 0;            // 记录活跃请求数最小的Invoker集合中，所有Invoker的权重值之和
        // The weight of the first least active invoker
        int firstWeight = 0;            // 记录活跃请求数最小的Invoker集合中，第一个Invoker的权重值
        // Every least active invoker has the same weight value?
        boolean sameWeight = true;          // 活跃请求数最小的集合中，所有Invoker的权重值是否相同


        // Filter out all the least active invokers
        for (int i = 0; i < length; i++) {      // 遍历所有Invoker，获取活跃请求数最小的Invoker集合
            Invoker<T> invoker = invokers.get(i);
            // Get the active number of the invoker
            int active = RpcStatus.getStatus(invoker.getUrl(), invocation.getMethodName()).getActive();     // 获取该Invoker的活跃请求数
            // Get the weight of the invoker's configuration. The default value is 100.
            int afterWarmup = getWeight(invoker, invocation);          // 获取该Invoker的权重
            // save for later use
            weights[i] = afterWarmup;
            // If it is the first invoker or the active number of the invoker is less than the current least active number
            if (leastActive == -1 || active < leastActive) {              // 比较活跃请求数
                // Reset the active number of the current invoker to the least active number
                // 当前的Invoker是第一个活跃请求数最小的Invoker，则记录如下信息
                leastActive = active;       // 重新记录最小的活跃请求数
                // Reset the number of least active invokers
                leastCount = 1;     // 重新记录活跃请求数最小的Invoker集合个数
                // Put the first least active invoker first in leastIndexes
                leastIndexes[0] = i;         // 重新记录Invoker
                // Reset totalWeight
                totalWeight = afterWarmup;       // 重新记录总权重值
                // Record the weight the first least active invoker
                firstWeight = afterWarmup;      // 该Invoker作为第一个Invoker，记录其权重值
                // Each invoke has the same weight (only one invoker here)
                sameWeight = true;      // 重新记录是否权重值相等
                // If current invoker's active value equals with leaseActive, then accumulating.
            } else if (active == leastActive) {
                // Record the index of the least active invoker in leastIndexes order
                leastIndexes[leastCount++] = i;      // 当前Invoker属于活跃请求数最小的Invoker集合  记录该Invoker的下标
                // Accumulate the total weight of the least active invoker
                totalWeight += afterWarmup;     // 更新总权重
                // If every invoker has the same weight?
                if (sameWeight && afterWarmup != firstWeight) {
                    sameWeight = false;     // 更新权重值是否相等
                }
            }
        }
        // Choose an invoker from all the least active invokers
        if (leastCount == 1) {        // 如果只有一个活跃请求数最小的Invoker对象，直接返回即可
            // If we got exactly one invoker having the least active value, return this invoker directly.
            return invokers.get(leastIndexes[0]);
        }
        // 下面按照RandomLoadBalance的逻辑，从活跃请求数最小的Invoker集合中，随机选择一个Invoker对象返回
        if (!sameWeight && totalWeight > 0) {
            // If (not every invoker has the same weight & at least one invoker's weight>0), select randomly based on 
            // totalWeight.
            int offsetWeight = ThreadLocalRandom.current().nextInt(totalWeight);
            // Return a invoker based on the random value.
            for (int i = 0; i < leastCount; i++) {
                int leastIndex = leastIndexes[i];
                offsetWeight -= weights[leastIndex];
                if (offsetWeight < 0) {
                    return invokers.get(leastIndex);
                }
            }
        }
        // If all invokers have the same weight value or totalWeight=0, return evenly.
        return invokers.get(leastIndexes[ThreadLocalRandom.current().nextInt(leastCount)]);
    }
}
