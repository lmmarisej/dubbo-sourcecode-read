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

import java.util.Arrays;
import java.util.Objects;

/**
 * 将相应类型的二维数组拍扁平化为同类型的一维数组
 */
public class IntArrayMerger implements Merger<int[]> {

    @Override
    public int[] merge(int[]... items) {
        if (ArrayUtils.isEmpty(items)) {              // 检测传入的多个int[]不能为空
            return new int[0];
        }
        return Arrays.stream(items).filter(Objects::nonNull)    // 直接使用Stream的API将多个int[]数组扁平化成一个int[]数组
                .flatMapToInt(Arrays::stream)
                .toArray();
    }

}
