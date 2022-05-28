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
package org.apache.dubbo.remoting;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.Adaptive;
import org.apache.dubbo.common.extension.ExtensionScope;
import org.apache.dubbo.common.extension.SPI;

/**
 * 针对每个支持的 NIO 库，都有一个 Transporter 接口实现，散落在各个 dubbo-remoting-* 实现模块中。
 *
 * 返回 NIO 库对应的 RemotingServer 实现和 Client 实现。
 *
 * 有了 Transporter 层之后，我们可以通过 Dubbo SPI 修改使用的具体 Transporter 扩展实现，
 * 从而切换到不同的 Client 和 RemotingServer 实现，达到底层 NIO 库切换的目的，而且无须修改任何代码。
 *
 * 即使有更先进的 NIO 库出现，我们也只需要开发相应的 dubbo-remoting-* 实现模块提供 Transporter、Client、RemotingServer
 * 等核心接口的实现，即可接入，完全符合开放-封闭原则。
 *
 * Transporter. (SPI, Singleton, ThreadSafe)
 * <p>
 * <a href="http://en.wikipedia.org/wiki/Transport_Layer">Transport Layer</a>
 * <a href="http://en.wikipedia.org/wiki/Client%E2%80%93server_model">Client/Server</a>
 *
 * @see org.apache.dubbo.remoting.Transporters
 */
@SPI(value = "netty", scope = ExtensionScope.FRAMEWORK)
public interface Transporter {

    /**
     * Bind a server.
     *
     * @param url     server url
     * @param handler
     * @return server
     * @throws RemotingException
     * @see org.apache.dubbo.remoting.Transporters#bind(URL, ChannelHandler...)
     */
    // 先后根据“server”“transporter”的值确定 RemotingServer 的扩展实现类，
    // 先后根据“client”“transporter”的值确定 Client 接口的扩展实现。
    @Adaptive({Constants.SERVER_KEY, Constants.TRANSPORTER_KEY})
    RemotingServer bind(URL url, ChannelHandler handler) throws RemotingException;

    /**
     * Connect to a server.
     * <pre> {@code
     * public class Transporter$Adaptive implements Transporter {
     *     public org.apache.dubbo.remoting.Client connect(URL arg0, ChannelHandler arg1) throws RemotingException {
     *         // 必须传递 URL 参数
     *         if (arg0 == null) throw new IllegalArgumentException("url == null");
     *
     *         URL url = arg0;
     *
     *         // 确定扩展名，优先从 URL 中的 client 参数获取，其次是 transporter 参数，这两个参数名称由 @Adaptive 注解指定，最后是 @SPI 注解中的默认值 。
     *         String extName = url.getParameter("client",url.getParameter("transporter", "netty"));
     *
     *         if (extName == null) throw new IllegalStateException("...");
     *
     *         // 通过ExtensionLoader加载Transporter接口的指定扩展实现
     *         Transporter extension = (Transporter) ExtensionLoader
     *               .getExtensionLoader(Transporter.class)
     *                     .getExtension(extName);
     *         return extension.connect(arg0, arg1);
     *     }
     *     // 省略bind()方法
     * }
     * }</pre>
     *
     * @param url     server url
     * @param handler
     * @return client
     * @throws RemotingException
     * @see org.apache.dubbo.remoting.Transporters#connect(URL, ChannelHandler...)
     */
    @Adaptive({Constants.CLIENT_KEY, Constants.TRANSPORTER_KEY})
    Client connect(URL url, ChannelHandler handler) throws RemotingException;

}
