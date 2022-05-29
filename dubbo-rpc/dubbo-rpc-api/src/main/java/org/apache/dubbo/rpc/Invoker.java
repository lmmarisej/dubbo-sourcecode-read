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

import org.apache.dubbo.common.Node;

/**
 * Proxy 底层会通过 Invoker 完成网络调用。
 *
 * 调用 -> proxy -> invoker
 *
 * 一些实现了 Protocol 接口以及 Invoker 接口的抽象类位于 protocol 包之中，
 * 它们主要是为 Protocol 接口的具体实现以及 Invoker 接口的具体实现提供一些公共逻辑。
 * <p>
 * Dubbo 中的很多设计思路都会向 Invoker 这个概念靠拢。
 * <p>
 * Invoker. (API/SPI, Prototype, ThreadSafe)
 *
 * @see org.apache.dubbo.rpc.Protocol#refer(Class, org.apache.dubbo.common.URL)
 * @see org.apache.dubbo.rpc.InvokerListener
 * @see org.apache.dubbo.rpc.protocol.AbstractInvoker
 */
public interface Invoker<T> extends Node {

    /**
     * 服务接口
     *
     * @return service interface.
     */
    Class<T> getInterface();

    /**
     * 进行一次调用，也有人称之为一次"会话"，你可以理解为一次调用
     *
     * @param invocation
     * @return result
     * @throws RpcException
     */
    Result invoke(Invocation invocation) throws RpcException;

    /**
     * destroy all
     */
    default void destroyAll() {
        destroy();
    }

}
