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
package org.apache.dubbo.rpc.cluster.configurator;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.remoting.Constants;
import org.apache.dubbo.rpc.cluster.Configurator;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.dubbo.common.constants.CommonConstants.ANYHOST_VALUE;
import static org.apache.dubbo.common.constants.CommonConstants.ANY_VALUE;
import static org.apache.dubbo.common.constants.CommonConstants.APPLICATION_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.CONSUMER;
import static org.apache.dubbo.common.constants.CommonConstants.ENABLED_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.GROUP_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.INTERFACES;
import static org.apache.dubbo.common.constants.CommonConstants.PROVIDER;
import static org.apache.dubbo.common.constants.CommonConstants.SIDE_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.VERSION_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.CATEGORY_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.COMPATIBLE_CONFIG_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.DYNAMIC_KEY;
import static org.apache.dubbo.rpc.cluster.Constants.CONFIG_VERSION_KEY;
import static org.apache.dubbo.rpc.cluster.Constants.OVERRIDE_PROVIDERS_KEY;

/**
 * 写入注册中心 configurators 中的动态配置有 override 和 absent 两种协议。
 *
 * override，表示采用覆盖方式。Dubbo 支持 override 和 absent 两种协议，我们也可以通过 SPI 的方式进行扩展。
 */
public abstract class AbstractConfigurator implements Configurator {

    private static final String TILDE = "~";

    private final URL configuratorUrl;         // 获取该 Configurator 对象对应的配置 URL，例如前文介绍的 override 协议 URL

    public AbstractConfigurator(URL url) {
        if (url == null) {
            throw new IllegalArgumentException("configurator url == null");
        }
        this.configuratorUrl = url;
    }

    @Override
    public URL getUrl() {
        return configuratorUrl;
    }

    @Override
    public URL configure(URL url) {
        // 这里会根据配置URL的enabled参数以及host决定该URL是否可用，同时还会根据原始URL是否为空以及原始URL的host是否为空，决定当前是否执行后续覆盖逻辑
        if (!configuratorUrl.getParameter(ENABLED_KEY, true) || configuratorUrl.getHost() == null || url == null || url.getHost() == null) {
            return url;
        }
        /*
         * 针对2.7.0之后版本，这里添加了一个 configVersion 参数作为区分
         */
        String apiVersion = configuratorUrl.getParameter(CONFIG_VERSION_KEY);
        if (StringUtils.isNotEmpty(apiVersion)) {
            String currentSide = url.getSide();
            String configuratorSide = configuratorUrl.getSide();
            // 根据配置URL中的side参数以及原始URL中的side参数值进行匹配
            if (currentSide.equals(configuratorSide) && CONSUMER.equals(configuratorSide) && 0 == configuratorUrl.getPort()) {
                url = configureIfMatch(NetUtils.getLocalHost(), url);
            } else if (currentSide.equals(configuratorSide) && PROVIDER.equals(configuratorSide) &&
                    url.getPort() == configuratorUrl.getPort()) {
                url = configureIfMatch(url.getHost(), url);
            }
        }
        /*
         * This else branch is deprecated and is left only to keep compatibility with versions before 2.7.0
         */
        else {
            url = configureDeprecated(url);      // 2.7.0版本之前对配置的处理
        }
        return url;
    }

    @Deprecated
    private URL configureDeprecated(URL url) {
        // If override url has port, means it is a provider address. We want to control a specific provider with this override url, it may take effect on the specific provider instance or on consumers holding this provider instance.
        // 如果配置URL中的端口不为空，则是针对Provider的，需要判断原始URL的端口，两者端口相同，才能执行configureIfMatch()方法中的配置方法
        if (configuratorUrl.getPort() != 0) {
            if (url.getPort() == configuratorUrl.getPort()) {
                return configureIfMatch(url.getHost(), url);
            }
        } else {
            /*
             *  override url don't have a port, means the ip override url specify is a consumer address or 0.0.0.0.
             *  1.If it is a consumer ip address, the intention is to control a specific consumer instance, it must takes effect at the consumer side, any provider received this override url should ignore.
             *  2.If the ip is 0.0.0.0, this override url can be used on consumer, and also can be used on provider.
             */
            // 如果没有指定端口，则该配置 URL 要么是针对 Consumer 的，要么是针对任意 URL 的（即 host 为0.0.0.0）
            // 如果原始 URL 属于 Consumer，则使用 Consumer 的 host 进行匹配
            if (url.getSide(PROVIDER).equals(CONSUMER)) {
                // NetUtils.getLocalHost is the ip address consumer registered to registry.
                return configureIfMatch(NetUtils.getLocalHost(), url);
            } else if (url.getSide(CONSUMER).equals(PROVIDER)) {
                // take effect on all providers, so address must be 0.0.0.0, otherwise it won't flow to this if branch
                return configureIfMatch(ANYHOST_VALUE, url);     // 如果是 Provider URL，则用 0.0.0.0 来配置
            }
        }
        return url;
    }

    // 重写原始 URL
    private URL configureIfMatch(String host, URL url) {
        if (ANYHOST_VALUE.equals(configuratorUrl.getHost()) || host.equals(configuratorUrl.getHost())) {
            // TODO, to support wildcards
            String providers = configuratorUrl.getParameter(OVERRIDE_PROVIDERS_KEY);
            if (StringUtils.isEmpty(providers) || providers.contains(url.getAddress()) || providers.contains(ANYHOST_VALUE)) {
                String configApplication = configuratorUrl.getApplication(configuratorUrl.getUsername());
                String currentApplication = url.getApplication(url.getUsername());
                if (configApplication == null || ANY_VALUE.equals(configApplication)
                        || configApplication.equals(currentApplication)) {      // 匹配application
                    // 排除不能动态修改的属性，其中包括category、check、dynamic、enabled还有以~开头的属性
                    Set<String> conditionKeys = new HashSet<String>();
                    conditionKeys.add(CATEGORY_KEY);
                    conditionKeys.add(Constants.CHECK_KEY);
                    conditionKeys.add(DYNAMIC_KEY);
                    conditionKeys.add(ENABLED_KEY);
                    conditionKeys.add(GROUP_KEY);
                    conditionKeys.add(VERSION_KEY);
                    conditionKeys.add(APPLICATION_KEY);
                    conditionKeys.add(SIDE_KEY);
                    conditionKeys.add(CONFIG_VERSION_KEY);
                    conditionKeys.add(COMPATIBLE_CONFIG_KEY);
                    conditionKeys.add(INTERFACES);
                    for (Map.Entry<String, String> entry : configuratorUrl.getParameters().entrySet()) {
                        String key = entry.getKey();
                        String value = entry.getValue();
                        boolean startWithTilde = startWithTilde(key);
                        if (startWithTilde || APPLICATION_KEY.equals(key) || SIDE_KEY.equals(key)) {
                            if (startWithTilde) {
                                conditionKeys.add(key);
                            }
                            // 如果配置 URL 与原 URL 中以~开头的参数值不相同，则不使用该配置 URL 重写原 URL
                            if (value != null && !ANY_VALUE.equals(value)
                                    && !value.equals(url.getParameter(startWithTilde ? key.substring(1) : key))) {
                                return url;
                            }
                        }
                    }
                    // 移除配置 URL 不支持动态配置的参数之后，调用 Configurator 子类的 doConfigure 方法重新生成 URL
                    return doConfigure(url, configuratorUrl.removeParameters(conditionKeys));
                }
            }
        }
        return url;
    }

    private boolean startWithTilde(String key) {
        if (StringUtils.isNotEmpty(key) && key.startsWith(TILDE)) {
            return true;
        }
        return false;
    }

    protected abstract URL doConfigure(URL currentUrl, URL configUrl);

}
