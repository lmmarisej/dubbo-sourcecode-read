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

package org.apache.dubbo.rpc.cluster.merger;

import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.TypeUtils;
import org.apache.dubbo.rpc.cluster.Merger;
import org.apache.dubbo.rpc.model.ScopeModel;
import org.apache.dubbo.rpc.model.ScopeModelAware;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * MergeableClusterInvoker 使用默认 Merger 实现的时候，通过 MergerFactory 以及服务接口返回值类型（returnType），选择合适的 Merger 实现。
 */
public class MergerFactory implements ScopeModelAware {

    private static final Logger logger = LoggerFactory.getLogger(MergerFactory.class);

    // 缓存服务接口返回值类型与 Merger 实例之间的映射关系。
    private final ConcurrentMap<Class<?>, Merger<?>> MERGER_CACHE = new ConcurrentHashMap<>();
    private ScopeModel scopeModel;

    @Override
    public void setScopeModel(ScopeModel scopeModel) {
        this.scopeModel = scopeModel;
    }

    /**
     * 根据传入的 returnType 类型，从 MERGER_CACHE 缓存中查找相应的 Merger 实现。
     *
     * Find the merger according to the returnType class, the merger will
     * merge an array of returnType into one
     *
     * @param returnType the merger will return this type
     * @return the merger which merges an array of returnType into one, return null if not exist
     * @throws IllegalArgumentException if returnType is null
     */
    public <T> Merger<T> getMerger(Class<T> returnType) {
        if (returnType == null) {       // returnType为空，直接抛出异常
            throw new IllegalArgumentException("returnType is null");
        }

        if (CollectionUtils.isEmptyMap(MERGER_CACHE)) {
            loadMergers();
        }
        Merger merger = MERGER_CACHE.get(returnType);            // 获取元素类型对应的Merger实现
        if (merger == null && returnType.isArray()) {        // returnType 为数组类型
            merger = ArrayMerger.INSTANCE;
        }
        return merger;      // 如果returnType不是数组类型，则直接从MERGER_CACHE缓存查找对应的Merger实例
    }

    /**
     * 通过 Dubbo SPI 方式加载 Merger 接口全部扩展实现的名称，并填充到 MERGER_CACHE 集合中
     */
    private void loadMergers() {
        Set<String> names = scopeModel.getExtensionLoader(Merger.class)  // 获取Merger接口的所有扩展名称
            .getSupportedExtensions();
        for (String name : names) {      // 遍历所有Merger扩展实现
            Merger m = scopeModel.getExtensionLoader(Merger.class).getExtension(name);
            Class<?> actualTypeArg = getActualTypeArgument(m.getClass());
            if (actualTypeArg == null) {
                logger.warn("Failed to get actual type argument from merger " + m.getClass().getName());
                continue;
            }
            // 将Merger实例与对应returnType的映射关系记录到MERGER_CACHE集合中
            MERGER_CACHE.putIfAbsent(actualTypeArg, m);
        }
    }

    /**
     * get merger's actual type argument (same as return type)
     * @param mergerCls
     * @return
     */
    private Class<?> getActualTypeArgument(Class<? extends Merger> mergerCls) {
        Class<?> superClass = mergerCls;
        while (superClass != Object.class) {
            Type[] interfaceTypes = superClass.getGenericInterfaces();
            ParameterizedType mergerType;
            for (Type it : interfaceTypes) {
                if (it instanceof ParameterizedType
                    && (mergerType = ((ParameterizedType) it)).getRawType() == Merger.class) {
                    Type typeArg = mergerType.getActualTypeArguments()[0];
                    return TypeUtils.getRawClass(typeArg);
                }
            }

            superClass = superClass.getSuperclass();
        }

        return null;
    }
}
