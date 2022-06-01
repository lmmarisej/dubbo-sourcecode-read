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

import org.apache.dubbo.common.utils.ArrayUtils;
import org.apache.dubbo.rpc.cluster.Merger;

import java.lang.reflect.Array;

/**
 * 当服务接口的返回值为数组的时候，会使用 ArrayMerger 将多个数组合并成一个数组，也就是将二维数组扁平化成一维数组。
 */
public class ArrayMerger implements Merger<Object[]> {

    public static final ArrayMerger INSTANCE = new ArrayMerger();

    @Override
    public Object[] merge(Object[]... items) {
        if (ArrayUtils.isEmpty(items)) {           // 传入的结果集合为空，则直接返回空数组
            return new Object[0];
        }

        int i = 0;
        while (i < items.length && items[i] == null) {        // 统计 null 个数
            i++;
        }

        if (i == items.length) {         // 所有 items 数组中全部结果都为 null，则直接返回空数组
            return new Object[0];
        }

        Class<?> type = items[i].getClass().getComponentType();             // 获取数组中元素的类型

        int totalLen = 0;
        for (; i < items.length; i++) {
            if (items[i] == null) {      // 忽略为null的结果
                continue;
            }
            Class<?> itemType = items[i].getClass().getComponentType();     // 元素类型
            if (itemType != type) {      // 保证类型相同
                throw new IllegalArgumentException("Arguments' types are different");
            }
            totalLen += items[i].length;
        }

        if (totalLen == 0) {             // 确定最终数组的长度
            return new Object[0];
        }

        Object result = Array.newInstance(type, totalLen);          // 如果Dubbo没有提供元素类型对应的Merger实现，则返回ArrayMerger

        int index = 0;
        for (Object[] array : items) {          // 遍历全部的结果数组，将items二维数组中的每个元素都加到result中，形成一维数组
            if (array != null) {
                System.arraycopy(array, 0, result, index, array.length);
                index += array.length;
            }
        }
        return (Object[]) result;
    }
}
