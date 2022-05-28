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
package org.apache.dubbo.remoting.transport;

import org.apache.dubbo.common.Resetable;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.remoting.ChannelHandler;
import org.apache.dubbo.remoting.Codec;
import org.apache.dubbo.remoting.Codec2;
import org.apache.dubbo.remoting.Constants;
import org.apache.dubbo.remoting.transport.codec.CodecAdapter;
import org.apache.dubbo.rpc.model.FrameworkModel;

import static org.apache.dubbo.rpc.model.ScopeModelUtil.getFrameworkModel;

/**
 * AbstractEndpoint
 */
public abstract class AbstractEndpoint extends AbstractPeer implements Resetable {

    private static final Logger logger = LoggerFactory.getLogger(AbstractEndpoint.class);

    private Codec2 codec;

    private int connectTimeout;

    public AbstractEndpoint(URL url, ChannelHandler handler) {
        super(url, handler);
        this.codec = getChannelCodec(url);      // 根据URL中的codec参数值，确定此处具体的Codec2实现类
        // 根据URL中的timeout参数确定timeout字段的值，默认1000
        this.connectTimeout = url.getPositiveParameter(Constants.CONNECT_TIMEOUT_KEY, Constants.DEFAULT_CONNECT_TIMEOUT);
    }

    protected static Codec2 getChannelCodec(URL url) {
        String codecName = url.getParameter(Constants.CODEC_KEY);       // 基于 Dubbo SPI 选择其扩展实现
        if (StringUtils.isEmpty(codecName)) {
            // codec extension name must stay the same with protocol name
            codecName = url.getProtocol();
        }
        FrameworkModel frameworkModel = getFrameworkModel(url.getScopeModel());
        // 通过 ExtensionLoader 加载并实例化 Codec2 的具体扩展实现
        if (frameworkModel.getExtensionLoader(Codec2.class).hasExtension(codecName)) {
            return frameworkModel.getExtensionLoader(Codec2.class).getExtension(codecName);
        } else {
            return new CodecAdapter(frameworkModel.getExtensionLoader(Codec.class)      // 尝试从Codec这个老接口的扩展名中查找
                .getExtension(codecName));
        }
    }

    /**
     * 根据传入的 URL 参数重置 AbstractEndpoint 的三个字段
     */
    @Override
    public void reset(URL url) {
        if (isClosed()) {
            throw new IllegalStateException("Failed to reset parameters "
                + url + ", cause: Channel closed. channel: " + getLocalAddress());
        }

        try {
            if (url.hasParameter(Constants.CONNECT_TIMEOUT_KEY)) {
                int t = url.getParameter(Constants.CONNECT_TIMEOUT_KEY, 0);
                if (t > 0) {
                    this.connectTimeout = t;
                }
            }
        } catch (Throwable t) {
            logger.error(t.getMessage(), t);
        }

        try {
            if (url.hasParameter(Constants.CODEC_KEY)) {
                this.codec = getChannelCodec(url);
            }
        } catch (Throwable t) {
            logger.error(t.getMessage(), t);
        }
    }

    @Deprecated
    public void reset(org.apache.dubbo.common.Parameters parameters) {
        reset(getUrl().addParameters(parameters.getParameters()));
    }

    protected Codec2 getCodec() {
        return codec;
    }

    protected int getConnectTimeout() {
        return connectTimeout;
    }

}
