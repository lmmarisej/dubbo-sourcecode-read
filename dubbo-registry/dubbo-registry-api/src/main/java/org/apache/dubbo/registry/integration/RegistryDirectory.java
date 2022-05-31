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
package org.apache.dubbo.registry.integration;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.URLBuilder;
import org.apache.dubbo.common.config.configcenter.DynamicConfiguration;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.url.component.DubboServiceAddressURL;
import org.apache.dubbo.common.url.component.ServiceAddressURL;
import org.apache.dubbo.common.url.component.ServiceConfigURL;
import org.apache.dubbo.common.utils.Assert;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.common.utils.UrlUtils;
import org.apache.dubbo.registry.AddressListener;
import org.apache.dubbo.remoting.Constants;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Protocol;
import org.apache.dubbo.rpc.cluster.Configurator;
import org.apache.dubbo.rpc.cluster.Router;
import org.apache.dubbo.rpc.cluster.directory.StaticDirectory;
import org.apache.dubbo.rpc.cluster.router.state.BitList;
import org.apache.dubbo.rpc.cluster.support.ClusterUtils;
import org.apache.dubbo.rpc.model.ApplicationModel;
import org.apache.dubbo.rpc.model.ModuleModel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.apache.dubbo.common.constants.CommonConstants.APPLICATION_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.DISABLED_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.DUBBO_PROTOCOL;
import static org.apache.dubbo.common.constants.CommonConstants.ENABLED_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.PROTOCOL_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.SIDE_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.TAG_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.APP_DYNAMIC_CONFIGURATORS_CATEGORY;
import static org.apache.dubbo.common.constants.RegistryConstants.COMPATIBLE_CONFIG_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.CONFIGURATORS_CATEGORY;
import static org.apache.dubbo.common.constants.RegistryConstants.DEFAULT_CATEGORY;
import static org.apache.dubbo.common.constants.RegistryConstants.DEFAULT_HASHMAP_LOAD_FACTOR;
import static org.apache.dubbo.common.constants.RegistryConstants.DYNAMIC_CONFIGURATORS_CATEGORY;
import static org.apache.dubbo.common.constants.RegistryConstants.EMPTY_PROTOCOL;
import static org.apache.dubbo.common.constants.RegistryConstants.PROVIDERS_CATEGORY;
import static org.apache.dubbo.common.constants.RegistryConstants.ROUTERS_CATEGORY;
import static org.apache.dubbo.common.constants.RegistryConstants.ROUTE_PROTOCOL;
import static org.apache.dubbo.registry.Constants.CONFIGURATORS_SUFFIX;
import static org.apache.dubbo.rpc.Constants.MOCK_KEY;
import static org.apache.dubbo.rpc.cluster.Constants.ROUTER_KEY;
import static org.apache.dubbo.rpc.model.ScopeModelUtil.getApplicationModel;


/**
 * 维护的 Invoker 集合会随着注册中心中维护的注册信息动态发生变化，这就依赖了 ZooKeeper 等注册中心的推送能力
 */
public class RegistryDirectory<T> extends DynamicDirectory<T> {
    private static final Logger logger = LoggerFactory.getLogger(RegistryDirectory.class);

    private final ConsumerConfigurationListener consumerConfigurationListener;
    private ReferenceConfigurationListener referenceConfigurationListener;

    /**
     * Map<url, Invoker> cache service url to invoker mapping.
     * The initial value is null and the midway may be assigned to null, please use the local variable reference
     */
    protected volatile Map<URL, Invoker<T>> urlInvokerMap;

    /**
     * The initial value is null and the midway may be assigned to null, please use the local variable reference
     */
    protected volatile Set<URL> cachedInvokerUrls;
    private final ApplicationModel applicationModel;

    public RegistryDirectory(Class<T> serviceType, URL url) {
        super(serviceType, url);
        applicationModel = getApplicationModel(url.getScopeModel());
        consumerConfigurationListener = getConsumerConfigurationListener(url.getOrDefaultModuleModel());
    }

    @Override
    public void subscribe(URL url) {
        super.subscribe(url);
        consumerConfigurationListener.addNotifyListener(this);
        referenceConfigurationListener = new ReferenceConfigurationListener(url.getOrDefaultModuleModel(), this, url);
    }

    private ConsumerConfigurationListener getConsumerConfigurationListener(ModuleModel moduleModel) {
        return moduleModel.getBeanFactory().getOrRegisterBean(ConsumerConfigurationListener.class,
            type -> new ConsumerConfigurationListener(moduleModel));
    }

    @Override
    public void unSubscribe(URL url) {
        super.unSubscribe(url);
        consumerConfigurationListener.removeNotifyListener(this);
        referenceConfigurationListener.stop();
    }

    @Override
    public synchronized void notify(List<URL> urls) {
        if (isDestroyed()) {
            return;
        }

        // 按照category进行分类，分成configurators、routers、providers三类
        Map<String, List<URL>> categoryUrls = urls.stream()
                .filter(Objects::nonNull)
                .filter(this::isValidCategory)
                .filter(this::isNotCompatibleFor26x)
                .collect(Collectors.groupingBy(this::judgeCategory));

        List<URL> configuratorURLs = categoryUrls.getOrDefault(CONFIGURATORS_CATEGORY, Collections.emptyList());
        // 将 configurators 类型的 URL 转化为 Configurator，保存到 configurators 字段中；
        this.configurators = Configurator.toConfigurators(configuratorURLs).orElse(this.configurators);

        // 将 router 类型的 URL 转化为 Router，并通过 routerChain.addRouters() 方法添加 routerChain 中保存；
        List<URL> routerURLs = categoryUrls.getOrDefault(ROUTERS_CATEGORY, Collections.emptyList());
        toRouters(routerURLs).ifPresent(this::addRouters);

        // 将 provider 类型的 URL 转化为 Invoker 对象，并记录到 invokers 集合和 urlInvokerMap 集合中。
        List<URL> providerURLs = categoryUrls.getOrDefault(PROVIDERS_CATEGORY, Collections.emptyList());

        // 3.x added for extend URL address
        ExtensionLoader<AddressListener> addressListenerExtensionLoader = getUrl().getOrDefaultModuleModel().getExtensionLoader(AddressListener.class);
        List<AddressListener> supportedListeners = addressListenerExtensionLoader.getActivateExtension(getUrl(), (String[]) null);
        if (supportedListeners != null && !supportedListeners.isEmpty()) {
            for (AddressListener addressListener : supportedListeners) {
                providerURLs = addressListener.notify(providerURLs, getConsumerUrl(),this);
            }
        }
        refreshOverrideAndInvoker(providerURLs);    // 触发 AddressListener 监听器
    }

    @Override
    public boolean isServiceDiscovery() {
        return false;
    }

    private String judgeCategory(URL url) {
        if (UrlUtils.isConfigurator(url)) {
            return CONFIGURATORS_CATEGORY;
        } else if (UrlUtils.isRoute(url)) {
            return ROUTERS_CATEGORY;
        } else if (UrlUtils.isProvider(url)) {
            return PROVIDERS_CATEGORY;
        }
        return "";
    }

    // RefreshOverrideAndInvoker will be executed by registryCenter and configCenter, so it should be synchronized.
    private synchronized void refreshOverrideAndInvoker(List<URL> urls) {
        // mock zookeeper://xxx?mock=return null
        refreshInvoker(urls);
    }

    /**
     * Convert the invokerURL list to the Invoker Map. The rules of the conversion are as follows:
     * <ol>
     * <li> If URL has been converted to invoker, it is no longer re-referenced and obtained directly from the cache,
     * and notice that any parameter changes in the URL will be re-referenced.</li>
     * <li>If the incoming invoker list is not empty, it means that it is the latest invoker list.</li>
     * <li>If the list of incoming invokerUrl is empty, It means that the rule is only a override rule or a route
     * rule, which needs to be re-contrasted to decide whether to re-reference.</li>
     * </ol>
     *
     * @param invokerUrls this parameter can't be null
     */
    private void refreshInvoker(List<URL> invokerUrls) {
        Assert.notNull(invokerUrls, "invokerUrls should not be null");

        // 如果invokerUrls集合不为空，长度为1，并且协议为empty，则表示该服务的所有Provider都下线了，会销毁当前所有Provider对应的Invoker。
        if (invokerUrls.size() == 1
                && invokerUrls.get(0) != null
                && EMPTY_PROTOCOL.equals(invokerUrls.get(0).getProtocol())) {
            this.forbidden = true; // Forbid to access      // forbidden标记设置为true，后续请求将直接抛出异常
            routerChain.setInvokers(BitList.emptyList());    // 清空RouterChain中的Invoker集合
            destroyAllInvokers(); // Close all invokers     // 关闭所有Invoker对象
        } else {
            this.forbidden = false; // Allow to access      // forbidden标记设置为false，RegistryDirectory可以正常处理后续请求
            
            if (invokerUrls == Collections.<URL>emptyList()) {
                invokerUrls = new ArrayList<>();
            }
            // use local reference to avoid NPE as this.cachedInvokerUrls will be set null by destroyAllInvokers().
            Set<URL> localCachedInvokerUrls = this.cachedInvokerUrls;
            if (invokerUrls.isEmpty() && localCachedInvokerUrls != null) {
                logger.warn("Service" + serviceKey + " received empty address list with no EMPTY protocol set, trigger empty protection.");
                invokerUrls.addAll(localCachedInvokerUrls);
            } else {
                localCachedInvokerUrls = new HashSet<>();
                localCachedInvokerUrls.addAll(invokerUrls);//Cached invoker urls, convenient for comparison
                this.cachedInvokerUrls = localCachedInvokerUrls;
            }
            if (invokerUrls.isEmpty()) {
                return;
            }

            // use local reference to avoid NPE as this.urlInvokerMap will be set null concurrently at destroyAllInvokers().
            Map<URL, Invoker<T>> localUrlInvokerMap = this.urlInvokerMap;       // 保存本地引用
            // can't use local reference as oldUrlInvokerMap's mappings might be removed directly at toInvokers().
            Map<URL, Invoker<T>> oldUrlInvokerMap = null;
            if (localUrlInvokerMap != null) {
                // the initial capacity should be set greater than the maximum number of entries divided by the load factor to avoid resizing.
                oldUrlInvokerMap = new LinkedHashMap<>(Math.round(1 + localUrlInvokerMap.size() / DEFAULT_HASHMAP_LOAD_FACTOR));
                oldUrlInvokerMap.putAll(localUrlInvokerMap);
            }

            // 将 invokerUrls 转换为对应的 Invoker 映射关系
            Map<URL, Invoker<T>> newUrlInvokerMap = toInvokers(oldUrlInvokerMap, invokerUrls);// Translate url list to Invoker map

            /**
             * If the calculation is wrong, it is not processed.
             *
             * 1. The protocol configured by the client is inconsistent with the protocol of the server.
             *    eg: consumer protocol = dubbo, provider only has other protocol services(rest).
             * 2. The registration center is not robust and pushes illegal specification data.
             *
             */
            if (CollectionUtils.isEmptyMap(newUrlInvokerMap)) {
                // 如果invokerUrls集合为空，即providers目录未发生变更，则无须处理，结束本次更新服务提供者Invoker操作。
                logger.error(new IllegalStateException("urls to invokers error .invokerUrls.size :" +
                    invokerUrls.size() + ", invoker.size :0. urls :" + invokerUrls));
                return;
            }

            // 更新invokers字段和urlInvokerMap集合
            List<Invoker<T>> newInvokers = Collections.unmodifiableList(new ArrayList<>(newUrlInvokerMap.values()));
            // 针对multiGroup的特殊处理，合并多个group的Invoker
            this.setInvokers(multiGroup ? new BitList<>(toMergeInvokerList(newInvokers)) : new BitList<>(newInvokers));
            // pre-route and build cache
            routerChain.setInvokers(this.getInvokers());
            this.urlInvokerMap = newUrlInvokerMap;

            try {
                // 比较新旧两组Invoker集合，销毁掉已经下线的Invoker
                destroyUnusedInvokers(oldUrlInvokerMap, newUrlInvokerMap); // Close the unused Invoker
            } catch (Exception e) {
                logger.warn("destroyUnusedInvokers error. ", e);
            }

            // notify invokers refreshed
            this.invokersChanged();
        }
    }

    /**
     * 根据 multiGroup 的配置决定是否调用 toMergeInvokerList() 方法将每个 group 中的 Invoker 合并成一个 Invoker。
     */
    private List<Invoker<T>> toMergeInvokerList(List<Invoker<T>> invokers) {
        List<Invoker<T>> mergedInvokers = new ArrayList<>();
        Map<String, List<Invoker<T>>> groupMap = new HashMap<>();
        for (Invoker<T> invoker : invokers) {       // 按照group将Invoker分组
            String group = invoker.getUrl().getGroup("");
            groupMap.computeIfAbsent(group, k -> new ArrayList<>());
            groupMap.get(group).add(invoker);
        }

        if (groupMap.size() == 1) {     // 如果只有一个group，则直接使用该group分组对应的Invoker集合作为mergedInvokers
            mergedInvokers.addAll(groupMap.values().iterator().next());
        } else if (groupMap.size() > 1) {       // 将每个group对应的Invoker集合合并成一个Invoker
            for (List<Invoker<T>> groupList : groupMap.values()) {
                // 这里使用到StaticDirectory以及Cluster合并每个group中的Invoker
                StaticDirectory<T> staticDirectory = new StaticDirectory<>(groupList);
                staticDirectory.buildRouterChain();
                mergedInvokers.add(cluster.join(staticDirectory, false));
            }
        } else {
            mergedInvokers = invokers;
        }
        return mergedInvokers;
    }

    /**
     * @param urls
     * @return null : no routers ,do nothing
     * else :routers list
     */
    private Optional<List<Router>> toRouters(List<URL> urls) {
        if (urls == null || urls.isEmpty()) {
            return Optional.empty();
        }

        List<Router> routers = new ArrayList<>();
        for (URL url : urls) {
            if (EMPTY_PROTOCOL.equals(url.getProtocol())) {
                continue;
            }
            String routerType = url.getParameter(ROUTER_KEY);
            if (routerType != null && routerType.length() > 0) {
                url = url.setProtocol(routerType);
            }
            try {
                Router router = routerFactory.getRouter(url);
                if (!routers.contains(router)) {
                    routers.add(router);
                }
            } catch (Throwable t) {
                logger.error("convert router url to router error, url: " + url, t);
            }
        }

        return Optional.of(routers);
    }

    /**
     * Provider URL 转换成 Invoker 对象。
     *
     * 核心逻辑就是调用 Protocol.refer() 方法创建 Invoker 对象。
     *
     * Turn urls into invokers, and if url has been refer, will not re-reference.
     * the items that will be put into newUrlInvokeMap will be removed from oldUrlInvokerMap.
     *
     * @param oldUrlInvokerMap it might be modified during the process.
     * @param urls
     * @return invokers
     */
    private Map<URL, Invoker<T>> toInvokers(Map<URL, Invoker<T>> oldUrlInvokerMap, List<URL> urls) {
        Map<URL, Invoker<T>> newUrlInvokerMap = new ConcurrentHashMap<>(urls == null ? 1 : (int) (urls.size() / 0.75f + 1));
        if (urls == null || urls.isEmpty()) {        // urls 集合为空时，直接返回空 Map
            return newUrlInvokerMap;
        }
        String queryProtocols = this.queryMap.get(PROTOCOL_KEY);     // 获取Consumer端支持的协议，即protocol参数指定的协议
        for (URL providerUrl : urls) {
            // If protocol is configured at the reference side, only the matching protocol is selected
            if (queryProtocols != null && queryProtocols.length() > 0) {
                boolean accept = false;
                String[] acceptProtocols = queryProtocols.split(",");
                for (String acceptProtocol : acceptProtocols) {
                    if (providerUrl.getProtocol().equals(acceptProtocol)) {
                        accept = true;
                        break;
                    }
                }
                if (!accept) {      // 如果当前URL不支持Consumer端的协议，也就无法执行后续转换成Invoker的逻辑
                    continue;
                }
            }
            if (EMPTY_PROTOCOL.equals(providerUrl.getProtocol())) {
                continue;           // 跳过empty协议的URL
            }
            // 如果Consumer端不支持该URL的协议（这里通过SPI方式检测是否有对应的Protocol扩展实现），也会跳过该URL
            if (!getUrl().getOrDefaultFrameworkModel().getExtensionLoader(Protocol.class).hasExtension(providerUrl.getProtocol())) {
                logger.error(new IllegalStateException("Unsupported protocol " + providerUrl.getProtocol() +
                        " in notified url: " + providerUrl + " from registry " + getUrl().getAddress() +
                        " to consumer " + NetUtils.getLocalHost() + ", supported protocol: " +
                    getUrl().getOrDefaultFrameworkModel().getExtensionLoader(Protocol.class).getSupportedExtensions()));
                continue;
            }
            URL url = mergeUrl(providerUrl);        // 合并URL参数

            // Cache key is url that does not merge with consumer side parameters, regardless of how the consumer combines parameters, if the server url changes, then refer again
            // 匹配urlInvokerMap缓存中的Invoker对象，如果命中缓存，直接将Invoker添加到newUrlInvokerMap这个新集合中即可；
            // 如果未命中缓存，则创建新的Invoker对象，然后添加到newUrlInvokerMap这个新集合中
            Invoker<T> invoker = oldUrlInvokerMap == null ? null : oldUrlInvokerMap.remove(url);
            if (invoker == null) { // Not in the cache, refer again
                try {
                    boolean enabled = true;
                    if (url.hasParameter(DISABLED_KEY)) {       // 检测URL中的disable和enable参数，决定是否能够创建Invoker对象
                        enabled = !url.getParameter(DISABLED_KEY, false);
                    } else {
                        enabled = url.getParameter(ENABLED_KEY, true);
                    }
                    if (enabled) {
                        invoker = protocol.refer(serviceType, url);     // 这里通过Protocol.refer()方法创建对应的Invoker对象
                    }
                } catch (Throwable t) {
                    logger.error("Failed to refer invoker for interface:" + serviceType + ",url:(" + url + ")" + t.getMessage(), t);
                }
                if (invoker != null) { // Put new invoker in cache       // 将key和Invoker对象之间的映射关系记录到newUrlInvokerMap中
                    newUrlInvokerMap.put(url, invoker);
                }
            } else {            // 缓存命中，直接将 urlInvokerMap 中的 Invoker 转移到 newUrlInvokerMap 即可
                newUrlInvokerMap.put(url, invoker);
            }
        }
        return newUrlInvokerMap;
    }

    /**
     * 会将注册中心中 configurators 目录下的 URL（override 协议），以及服务治理控制台动态添加的配置与 Provider URL 进行合并，
     * 即覆盖 Provider URL 原有的一些信息。
     *
     * Merge url parameters. the order is: override > -D >Consumer > Provider
     *
     * @param providerUrl
     * @return
     */
    private URL mergeUrl(URL providerUrl) {
        // 首先，移除Provider URL中只在Provider端生效的属性，例如，threadname、threadpool、corethreads、threads、queues等参数。
        // 然后，用Consumer端的配置覆盖Provider URL的相应配置，其中，version、group、methods、timestamp等参数以Provider端的配置优先
        // 最后，合并Provider端和Consumer端配置的Filter以及Listener
        if (providerUrl instanceof ServiceAddressURL) {
            // 合并configurators类型的URL，configurators类型的URL又分为三类：
            // 第一类是注册中心Configurators目录下新增的URL(override协议)
            // 第二类是通过ConsumerConfigurationListener监听器(监听应用级别的配置)得到的动态配置
            // 第三类是通过ReferenceConfigurationListener监听器(监听服务级别的配置)得到的动态配置
            // 这里只需要先了解：除了注册中心的configurators目录下有配置信息之外，还有可以在服务治理控制台动态添加配置，
            // ConsumerConfigurationListener、ReferenceConfigurationListener监听器就是用来监听服务治理控制台的动态配置的
            // 至于服务治理控制台的具体使用，在后面详细介绍
            providerUrl = overrideWithConfigurator(providerUrl);
        } else {
            // 增加check=false，即只有在调用时，才检查Provider是否可用
            providerUrl = applicationModel.getBeanFactory().getBean(ClusterUtils.class).mergeUrl(providerUrl, queryMap); // Merge the consumer side parameters
            // 重新复制overrideDirectoryUrl，providerUrl在经过第一步参数合并后（包含override协议覆盖后的属性）赋值给overrideDirectoryUrl。
            providerUrl = overrideWithConfigurator(providerUrl);
            providerUrl = providerUrl.addParameter(Constants.CHECK_KEY, String.valueOf(false)); // Do not check whether the connection is successful or not, always create Invoker!
        }

        // FIXME, kept for mock
        if (providerUrl.hasParameter(MOCK_KEY) || providerUrl.getAnyMethodParameter(MOCK_KEY) != null) {
            providerUrl = providerUrl.removeParameter(TAG_KEY);
        }

        // 对Dubbo低版本的兼容处理逻辑
        if ((providerUrl.getPath() == null || providerUrl.getPath()
                .length() == 0) && DUBBO_PROTOCOL.equals(providerUrl.getProtocol())) { // Compatible version 1.0
            //fix by tony.chenl DUBBO-44
            String path = directoryUrl.getServiceInterface();
            if (path != null) {
                int i = path.indexOf('/');
                if (i >= 0) {
                    path = path.substring(i + 1);
                }
                i = path.lastIndexOf(':');
                if (i >= 0) {
                    path = path.substring(0, i);
                }
                providerUrl = providerUrl.setPath(path);
            }
        }
        return providerUrl;
    }

    private URL overrideWithConfigurator(URL providerUrl) {
        // override url with configurator from "override://" URL for dubbo 2.6 and before
        providerUrl = overrideWithConfigurators(this.configurators, providerUrl);

        // override url with configurator from "app-name.configurators"
        providerUrl = overrideWithConfigurators(consumerConfigurationListener.getConfigurators(), providerUrl);

        // override url with configurator from configurators from "service-name.configurators"
        if (referenceConfigurationListener != null) {
            providerUrl = overrideWithConfigurators(referenceConfigurationListener.getConfigurators(), providerUrl);
        }

        return providerUrl;
    }

    private URL overrideWithConfigurators(List<Configurator> configurators, URL url) {
        if (CollectionUtils.isNotEmpty(configurators)) {
            if (url instanceof DubboServiceAddressURL) {
                DubboServiceAddressURL interfaceAddressURL = (DubboServiceAddressURL) url;
                URL overriddenURL = interfaceAddressURL.getOverrideURL();
                if (overriddenURL == null) {
                    String appName = interfaceAddressURL.getApplication();
                    String side = interfaceAddressURL.getSide();
                    overriddenURL = URLBuilder.from(interfaceAddressURL)
                            .clearParameters()
                            .addParameter(APPLICATION_KEY, appName)
                            .addParameter(SIDE_KEY, side).build();
                }
                for (Configurator configurator : configurators) {
                    overriddenURL = configurator.configure(overriddenURL);
                }
                url = new DubboServiceAddressURL(
                        interfaceAddressURL.getUrlAddress(),
                        interfaceAddressURL.getUrlParam(),
                        interfaceAddressURL.getConsumerURL(),
                        (ServiceConfigURL) overriddenURL);
            } else {
                for (Configurator configurator : configurators) {
                    url = configurator.configure(url);
                }
            }
        }
        return url;
    }

    /**
     * Close all invokers
     */
    @Override
    protected void destroyAllInvokers() {
        Map<URL, Invoker<T>> localUrlInvokerMap = this.urlInvokerMap; // local reference
        if (!CollectionUtils.isEmptyMap(localUrlInvokerMap)) {
            for (Invoker<T> invoker : new ArrayList<>(localUrlInvokerMap.values())) {
                try {
                    invoker.destroyAll();
                } catch (Throwable t) {
                    logger.warn("Failed to destroy service " + serviceKey + " to provider " + invoker.getUrl(), t);
                }
            }
            localUrlInvokerMap.clear();
        }

        this.urlInvokerMap = null;
        this.cachedInvokerUrls = null;
        destroyInvokers();
    }

    private void destroyUnusedInvokers(Map<URL, Invoker<T>> oldUrlInvokerMap, Map<URL, Invoker<T>> newUrlInvokerMap) {
        if (CollectionUtils.isEmptyMap(newUrlInvokerMap)) {
            destroyAllInvokers();
            return;
        }

        if (CollectionUtils.isEmptyMap(oldUrlInvokerMap)) {
            return;
        }

        for (Map.Entry<URL, Invoker<T>> entry : oldUrlInvokerMap.entrySet()) {
            Invoker<T> invoker = entry.getValue();
            if (invoker != null) {
                try {
                    invoker.destroyAll();
                    if (logger.isDebugEnabled()) {
                        logger.debug("destroy invoker[" + invoker.getUrl() + "] success. ");
                    }
                } catch (Exception e) {
                    logger.warn("destroy invoker[" + invoker.getUrl() + "] failed. " + e.getMessage(), e);
                }
            }
        }

        logger.info("New url total size, " + newUrlInvokerMap.size() + ", destroyed total size " + oldUrlInvokerMap.size());
    }

    /**
     * Haomin: added for test purpose
     */
    public Map<URL, Invoker<T>> getUrlInvokerMap() {
        return urlInvokerMap;
    }

    private boolean isValidCategory(URL url) {
        String category = url.getCategory(DEFAULT_CATEGORY);
        if ((ROUTERS_CATEGORY.equals(category) || ROUTE_PROTOCOL.equals(url.getProtocol())) ||
                PROVIDERS_CATEGORY.equals(category) ||
                CONFIGURATORS_CATEGORY.equals(category) || DYNAMIC_CONFIGURATORS_CATEGORY.equals(category) ||
                APP_DYNAMIC_CONFIGURATORS_CATEGORY.equals(category)) {
            return true;
        }
        logger.warn("Unsupported category " + category + " in notified url: " + url + " from registry " +
                getUrl().getAddress() + " to consumer " + NetUtils.getLocalHost());
        return false;
    }

    private boolean isNotCompatibleFor26x(URL url) {
        return StringUtils.isEmpty(url.getParameter(COMPATIBLE_CONFIG_KEY));
    }

    private static class ReferenceConfigurationListener extends AbstractConfiguratorListener {
        private RegistryDirectory directory;
        private URL url;

        ReferenceConfigurationListener(ModuleModel moduleModel, RegistryDirectory directory, URL url) {
            super(moduleModel);
            this.directory = directory;
            this.url = url;
            this.initWith(DynamicConfiguration.getRuleKey(url) + CONFIGURATORS_SUFFIX);
        }

        void stop() {
            this.stopListen(DynamicConfiguration.getRuleKey(url) + CONFIGURATORS_SUFFIX);
        }

        @Override
        protected void notifyOverrides() {
            // to notify configurator/router changes
            directory.refreshOverrideAndInvoker(Collections.emptyList());
        }
    }

    private static class ConsumerConfigurationListener extends AbstractConfiguratorListener {
        List<RegistryDirectory> listeners = new ArrayList<>();

        ConsumerConfigurationListener(ModuleModel moduleModel) {
            super(moduleModel);
            this.initWith(moduleModel.getApplicationModel().getApplicationName() + CONFIGURATORS_SUFFIX);
        }

        void addNotifyListener(RegistryDirectory listener) {
            this.listeners.add(listener);
        }

        void removeNotifyListener(RegistryDirectory listener) {
            this.listeners.remove(listener);
        }

        @Override
        protected void notifyOverrides() {
            listeners.forEach(listener -> listener.refreshOverrideAndInvoker(Collections.emptyList()));
        }
    }

}
