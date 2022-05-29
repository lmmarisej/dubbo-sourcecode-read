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

import org.apache.dubbo.common.extension.ExtensionScope;
import org.apache.dubbo.common.extension.SPI;

/**
 * 服务发布的过程中，我们可以添加一些 Listener 来监听相应的事件。
 *
 * ExporterListener. (SPI, Singleton, ThreadSafe)
 */
@SPI(scope = ExtensionScope.FRAMEWORK)
public interface ExporterListener {

    /**
     * 当有服务发布的时候，会触发该方法
     */
    void exported(Exporter<?> exporter) throws RpcException;

    /**
     * 当有服务取消发布的时候，会触发该方法
     */
    void unexported(Exporter<?> exporter);

}
