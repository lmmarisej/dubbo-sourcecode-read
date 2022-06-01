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
package org.apache.dubbo.rpc.cluster;

import org.apache.dubbo.common.extension.SPI;

/**
 * 在 Dubbo 中提供了处理不同类型返回值的 Merger 实现，其中不仅有处理 boolean[]、byte[]、char[]、double[]、float[]、int[]、long[]、short[]
 * 等基础类型数组的 Merger 实现，还有处理 List、Set、Map 等集合类的 Merger 实现
 */
@SPI
public interface Merger<T> {

    T merge(T... items);

}
