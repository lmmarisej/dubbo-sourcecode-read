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
package org.apache.dubbo.rpc.filter.tps;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Invocation;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.apache.dubbo.rpc.Constants.DEFAULT_TPS_LIMIT_INTERVAL;
import static org.apache.dubbo.rpc.Constants.TPS_LIMIT_INTERVAL_KEY;
import static org.apache.dubbo.rpc.Constants.TPS_LIMIT_RATE_KEY;

/**
 * DefaultTPSLimiter is a default implementation for tps filter. It is an in memory based implementation for storing
 * tps information. It internally use
 *
 * @see org.apache.dubbo.rpc.filter.TpsLimitFilter
 */
public class DefaultTPSLimiter implements TPSLimiter {

    // 为每个 ServiceKey 维护了一个相应的 StatItem 对象
    private final ConcurrentMap<String, StatItem> stats = new ConcurrentHashMap<>();

    /**
     * 从 URL 中读取 tps 参数值（默认为 -1，即没有限流），对于需要限流的请求，会从 stats 集合中获取（或创建）相应 StatItem 对象，
     * 然后调用 StatItem 对象的isAllowable() 方法判断是否被限流
     */
    @Override
    public boolean isAllowable(URL url, Invocation invocation) {
        int rate = url.getMethodParameter(invocation.getMethodName(), TPS_LIMIT_RATE_KEY, -1);
        long interval = url.getMethodParameter(invocation.getMethodName(), TPS_LIMIT_INTERVAL_KEY, DEFAULT_TPS_LIMIT_INTERVAL);
        String serviceKey = url.getServiceKey();
        StatItem statItem = stats.get(serviceKey);
        if (rate > 0) {     // 需要限流，尝试从stats集合中获取相应的StatItem对象
            if (statItem == null) {
                stats.putIfAbsent(serviceKey, new StatItem(serviceKey, rate, interval));
                statItem = stats.get(serviceKey);
            } else {        // URL中参数发生变化时，会重建对应的StatItem
                //rate or interval has changed, rebuild
                if (statItem.getRate() != rate || statItem.getInterval() != interval) {
                    stats.put(serviceKey, new StatItem(serviceKey, rate, interval));
                    statItem = stats.get(serviceKey);
                }
            }
            return statItem.isAllowable();
        } else {        // 不需要限流，则从 stats 集合中清除相应的 StatItem 对象
            if (statItem != null) {
                stats.remove(serviceKey);
            }
        }

        return true;
    }

}
