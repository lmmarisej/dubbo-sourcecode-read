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
package org.apache.dubbo.config;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.Version;
import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.constants.RegistryConstants;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.url.component.ServiceConfigURL;
import org.apache.dubbo.common.utils.*;
import org.apache.dubbo.config.annotation.Reference;
import org.apache.dubbo.config.support.Parameter;
import org.apache.dubbo.config.utils.ConfigValidationUtils;
import org.apache.dubbo.metadata.ServiceNameMapping;
import org.apache.dubbo.registry.client.metadata.MetadataUtils;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Protocol;
import org.apache.dubbo.rpc.ProxyFactory;
import org.apache.dubbo.rpc.cluster.Cluster;
import org.apache.dubbo.rpc.cluster.directory.StaticDirectory;
import org.apache.dubbo.rpc.cluster.support.ClusterUtils;
import org.apache.dubbo.rpc.cluster.support.registry.ZoneAwareCluster;
import org.apache.dubbo.rpc.model.*;
import org.apache.dubbo.rpc.protocol.injvm.InjvmProtocol;
import org.apache.dubbo.rpc.service.GenericService;
import org.apache.dubbo.rpc.stub.StubSuppliers;
import org.apache.dubbo.rpc.support.ProtocolUtils;

import java.util.*;

import static org.apache.dubbo.common.constants.CommonConstants.*;
import static org.apache.dubbo.common.constants.RegistryConstants.SUBSCRIBED_SERVICE_NAMES_KEY;
import static org.apache.dubbo.common.utils.NetUtils.isInvalidLocalHost;
import static org.apache.dubbo.common.utils.StringUtils.splitToSet;
import static org.apache.dubbo.config.Constants.DUBBO_IP_TO_REGISTRY;
import static org.apache.dubbo.registry.Constants.CONSUMER_PROTOCOL;
import static org.apache.dubbo.registry.Constants.REGISTER_IP_KEY;
import static org.apache.dubbo.rpc.Constants.GENERIC_KEY;
import static org.apache.dubbo.rpc.Constants.LOCAL_PROTOCOL;
import static org.apache.dubbo.rpc.cluster.Constants.PEER_KEY;
import static org.apache.dubbo.rpc.cluster.Constants.REFER_KEY;

/**
 * 服务引用的真正入口，服务引用的核心实现在 ReferenceConfig 之中。
 * <p>
 * 一个 ReferenceConfig 对象对应一个服务接口，每个 ReferenceConfig 对象中都封装了与注册中心的网络连接，以及与 Provider 的网络连接。
 * <p>
 * Please avoid using this class for any new application,
 * use {@link ReferenceConfigBase} instead.
 */
public class ReferenceConfig<T> extends ReferenceConfigBase<T> {

    public static final Logger logger = LoggerFactory.getLogger(ReferenceConfig.class);

    /**
     * The {@link Protocol} implementation with adaptive functionality,it will be different in different scenarios.
     * A particular {@link Protocol} implementation is determined by the protocol attribute in the {@link URL}.
     * For example:
     *
     * <li>when the url is registry://224.5.6.7:1234/org.apache.dubbo.registry.RegistryService?application=dubbo-sample,
     * then the protocol is <b>RegistryProtocol</b></li>
     *
     * <li>when the url is dubbo://224.5.6.7:1234/org.apache.dubbo.config.api.DemoService?application=dubbo-sample, then
     * the protocol is <b>DubboProtocol</b></li>
     * <p>
     * Actually，when the {@link ExtensionLoader} init the {@link Protocol} instants,it will automatically wrap three
     * layers, and eventually will get a <b>ProtocolSerializationWrapper</b> or <b>ProtocolFilterWrapper</b> or <b>ProtocolListenerWrapper</b>
     */
    private Protocol protocolSPI;

    /**
     * A {@link ProxyFactory} implementation that will generate a reference service's proxy,the JavassistProxyFactory is
     * its default implementation
     * <p>
     * 适配器，选择合适的 ProxyFactory 扩展实现，将 Invoker 包装成服务接口的代理对象。
     */
    private ProxyFactory proxyFactory;

    private ConsumerModel consumerModel;

    /**
     * The interface proxy reference
     */
    private transient volatile T ref;        // ref 指向了服务的代理对象

    /**
     * The invoker of the reference service
     */
    private transient volatile Invoker<?> invoker;

    /**
     * The flag whether the ReferenceConfig has been initialized
     */
    private transient volatile boolean initialized;

    /**
     * whether this ReferenceConfig has been destroyed
     */
    private transient volatile boolean destroyed;

    /**
     * The service names that the Dubbo interface subscribed.
     *
     * @since 2.7.8
     */
    private String services;

    public ReferenceConfig() {
        super();
    }

    public ReferenceConfig(ModuleModel moduleModel) {
        super(moduleModel);
    }

    public ReferenceConfig(Reference reference) {
        super(reference);
    }

    public ReferenceConfig(ModuleModel moduleModel, Reference reference) {
        super(moduleModel, reference);
    }

    @Override
    protected void postProcessAfterScopeModelChanged(ScopeModel oldScopeModel, ScopeModel newScopeModel) {
        super.postProcessAfterScopeModelChanged(oldScopeModel, newScopeModel);

        protocolSPI = this.getExtensionLoader(Protocol.class).getAdaptiveExtension();
        proxyFactory = this.getExtensionLoader(ProxyFactory.class).getAdaptiveExtension();
    }

    /**
     * Get a string presenting the service names that the Dubbo interface subscribed.
     * If it is a multiple-values, the content will be a comma-delimited String.
     *
     * @return non-null
     * @see RegistryConstants#SUBSCRIBED_SERVICE_NAMES_KEY
     * @since 2.7.8
     */
    @Deprecated
    @Parameter(key = SUBSCRIBED_SERVICE_NAMES_KEY)
    public String getServices() {
        return services;
    }

    /**
     * It's an alias method for {@link #getServices()}, but the more convenient.
     *
     * @return the String {@link List} presenting the Dubbo interface subscribed
     * @since 2.7.8
     */
    @Deprecated
    @Parameter(excluded = true)
    public Set<String> getSubscribedServices() {
        return splitToSet(getServices(), COMMA_SEPARATOR_CHAR);
    }

    /**
     * Set the service names that the Dubbo interface subscribed.
     *
     * @param services If it is a multiple-values, the content will be a comma-delimited String.
     * @since 2.7.8
     */
    public void setServices(String services) {
        this.services = services;
    }

    @Override
    public T get() {
        if (destroyed) {         // 检测当前ReferenceConfig状态
            throw new IllegalStateException("The invoker of ReferenceConfig(" + url + ") has already destroyed!");
        }

        if (ref == null) {
            // ensure start module, compatible with old api usage
            getScopeModel().getDeployer().start();

            synchronized (this) {
                if (ref == null) {
                    init();             // 初始化 ref 字段
                }
            }
        }

        return ref;
    }

    @Override
    public synchronized void destroy() {
        super.destroy();
        if (destroyed) {
            return;
        }
        destroyed = true;
        try {
            if (invoker != null) {
                invoker.destroy();
            }
        } catch (Throwable t) {
            logger.warn("Unexpected error occurred when destroy invoker of ReferenceConfig(" + url + ").", t);
        }
        invoker = null;
        ref = null;
        if (consumerModel != null) {
            ModuleServiceRepository repository = getScopeModel().getServiceRepository();
            repository.unregisterConsumer(consumerModel);
        }
    }

    /**
     * 对服务引用的配置进行处理，以保证配置的正确性。
     */
    protected synchronized void init() {
        if (initialized) {      // 检测ReferenceConfig的初始化状态
            return;
        }
        initialized = true;

        if (!this.isRefreshed()) {
            this.refresh();
        }

        // init serviceMetadata
        initServiceMetadata(consumer);

        serviceMetadata.setServiceType(getServiceInterfaceClass());
        // TODO, uncomment this line once service key is unified
        serviceMetadata.setServiceKey(URL.buildKey(interfaceName, group, version));

        Map<String, String> referenceParameters = appendConfig();       // 省略其他配置的检查
        // init service-application mapping
        initServiceAppsMapping(referenceParameters);

        ModuleServiceRepository repository = getScopeModel().getServiceRepository();
        ServiceDescriptor serviceDescriptor;
        if (CommonConstants.NATIVE_STUB.equals(getProxy())) {
            serviceDescriptor = StubSuppliers.getServiceDescriptor(interfaceName);       // 添加interface参数
            repository.registerService(serviceDescriptor);
        } else {
            serviceDescriptor = repository.registerService(interfaceClass);
        }
        consumerModel = new ConsumerModel(serviceMetadata.getServiceKey(), proxy, serviceDescriptor, this,
            getScopeModel(), serviceMetadata, createAsyncMethodInfo());

        repository.registerConsumer(consumerModel);

        serviceMetadata.getAttachments().putAll(referenceParameters);

        ref = createProxy(referenceParameters);     // 创建代理

        serviceMetadata.setTarget(ref);
        serviceMetadata.addAttribute(PROXY_CLASS_REF, ref);

        consumerModel.setProxyObject(ref);
        consumerModel.initMethodModels();

        checkInvokerAvailable();
    }

    private void initServiceAppsMapping(Map<String, String> referenceParameters) {
        ServiceNameMapping serviceNameMapping = ServiceNameMapping.getDefaultExtension(getScopeModel());
        URL url = new ServiceConfigURL(LOCAL_PROTOCOL, LOCALHOST_VALUE, 0, interfaceName, referenceParameters);
        serviceNameMapping.initInterfaceAppMapping(url);
    }

    /**
     * convert and aggregate async method info
     *
     * @return Map<String, AsyncMethodInfo>
     */
    private Map<String, AsyncMethodInfo> createAsyncMethodInfo() {
        Map<String, AsyncMethodInfo> attributes = null;
        if (CollectionUtils.isNotEmpty(getMethods())) {
            attributes = new HashMap<>(16);
            for (MethodConfig methodConfig : getMethods()) {
                AsyncMethodInfo asyncMethodInfo = methodConfig.convertMethodConfig2AsyncInfo();
                if (asyncMethodInfo != null) {
                    attributes.put(methodConfig.getName(), asyncMethodInfo);
                }
            }
        }

        return attributes;
    }

    /**
     * Append all configuration required for service reference.
     *
     * @return reference parameters
     */
    private Map<String, String> appendConfig() {
        Map<String, String> map = new HashMap<>(16);

        map.put(INTERFACE_KEY, interfaceName);
        map.put(SIDE_KEY, CONSUMER_SIDE);           // 添加side参数

        ReferenceConfigBase.appendRuntimeParameters(map);         // 添加Dubbo版本、release参数、timestamp参数、pid参数

        if (!ProtocolUtils.isGeneric(generic)) {
            String revision = Version.getVersion(interfaceClass, version);
            if (StringUtils.isNotEmpty(revision)) {
                map.put(REVISION_KEY, revision);
            }

            String[] methods = methods(interfaceClass);
            if (methods.length == 0) {
                logger.warn("No method found in service interface " + interfaceClass.getName());
                map.put(METHODS_KEY, ANY_VALUE);
            } else {
                map.put(METHODS_KEY, StringUtils.join(new HashSet<>(Arrays.asList(methods)), COMMA_SEPARATOR));
            }
        }

        AbstractConfig.appendParameters(map, getApplication());
        AbstractConfig.appendParameters(map, getModule());
        AbstractConfig.appendParameters(map, consumer);
        AbstractConfig.appendParameters(map, this);
        appendMetricsCompatible(map);

        String hostToRegistry = ConfigUtils.getSystemProperty(DUBBO_IP_TO_REGISTRY);
        if (StringUtils.isEmpty(hostToRegistry)) {
            hostToRegistry = NetUtils.getLocalHost();
        } else if (isInvalidLocalHost(hostToRegistry)) {
            throw new IllegalArgumentException(
                "Specified invalid registry ip from property:" + DUBBO_IP_TO_REGISTRY + ", value:" + hostToRegistry);
        }

        map.put(REGISTER_IP_KEY, hostToRegistry);        // 添加 ip 参数

        if (CollectionUtils.isNotEmpty(getMethods())) {
            for (MethodConfig methodConfig : getMethods()) {
                AbstractConfig.appendParameters(map, methodConfig, methodConfig.getName());
                String retryKey = methodConfig.getName() + ".retry";
                if (map.containsKey(retryKey)) {
                    String retryValue = map.remove(retryKey);
                    if ("false".equals(retryValue)) {
                        map.put(methodConfig.getName() + ".retries", "0");
                    }
                }
            }
        }

        return map;
    }

    /**
     * 处理了多种服务引用的场景，例如，直连单个/多个Provider、单个/多个注册中心。
     * <p>
     * 1.一是通过 Protocol 适配器选择合适的 Protocol 扩展实现创建 Invoker 对象；
     * 2.二是通过 ProxyFactory 适配器选择合适的 ProxyFactory 创建代理对象。
     */
    @SuppressWarnings({"unchecked"})
    private T createProxy(Map<String, String> referenceParameters) {
        if (shouldJvmRefer(referenceParameters)) {  // 根据传入的参数集合判断协议是否为 injvm 协议，如果是，直接通过 InjvmProtocol 引用服务。
            createInvokerForLocal(referenceParameters);
        } else {
            urls.clear();
            if (StringUtils.isNotEmpty(url)) {
                // user specified URL, could be peer-to-peer address, or register center's address.
                parseUrl(referenceParameters);      // url 参数解析
            } else {
                // if protocols not in jvm checkRegistry
                if (!LOCAL_PROTOCOL.equalsIgnoreCase(getProtocol())) {
                    aggregateUrlFromRegistry(referenceParameters);
                }
            }
            createInvokerForRemote();       // 远程 invoker
        }

        if (logger.isInfoEnabled()) {
            logger.info("Referred dubbo service: [" + referenceParameters.get(INTERFACE_KEY) + "]." +
                (Boolean.parseBoolean(referenceParameters.get(GENERIC_KEY)) ?
                    " it's GenericService reference" : " it's not GenericService reference"));
        }

        URL consumerUrl = new ServiceConfigURL(        // 元数据处理相关的逻辑
            CONSUMER_PROTOCOL,
            referenceParameters.get(REGISTER_IP_KEY),
            0,
            referenceParameters.get(INTERFACE_KEY),
            referenceParameters
        );
        consumerUrl = consumerUrl.setScopeModel(getScopeModel());
        consumerUrl = consumerUrl.setServiceModel(consumerModel);
        MetadataUtils.publishServiceDefinition(consumerUrl, consumerModel.getServiceModel(), getApplicationModel());

        // create service proxy
        // 通过ProxyFactory适配器选择合适的ProxyFactory扩展实现，创建代理对象
        return (T) proxyFactory.getProxy(invoker, ProtocolUtils.isGeneric(generic));
    }

    /**
     * Make a local reference, create a local invoker.
     *
     * @param referenceParameters
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private void createInvokerForLocal(Map<String, String> referenceParameters) {
        URL url = new ServiceConfigURL(LOCAL_PROTOCOL, LOCALHOST_VALUE, 0, interfaceClass.getName(), referenceParameters);
        url = url.setScopeModel(getScopeModel());
        url = url.setServiceModel(consumerModel);
        Invoker<?> withFilter = protocolSPI.refer(interfaceClass, url);
        // Local Invoke ( Support Cluster Filter / Filter )
        List<Invoker<?>> invokers = new ArrayList<>();
        invokers.add(withFilter);
        invoker = Cluster.getCluster(url.getScopeModel(), Cluster.DEFAULT).join(new StaticDirectory(url, invokers), true);

        if (logger.isInfoEnabled()) {
            logger.info("Using in jvm service " + interfaceClass.getName());
        }
    }

    /**
     * Parse the directly configured url.
     */
    private void parseUrl(Map<String, String> referenceParameters) {
        String[] us = SEMICOLON_SPLIT_PATTERN.split(url);       // 配置多个URL的时候，会用分号进行切分
        if (ArrayUtils.isNotEmpty(us)) {        // url不为空，表明用户可能想进行点对点调用
            for (String u : us) {
                URL url = URL.valueOf(u);
                if (StringUtils.isEmpty(url.getPath())) {
                    url = url.setPath(interfaceName);        // 设置接口完全限定名为URL Path
                }
                url = url.setScopeModel(getScopeModel());
                url = url.setServiceModel(consumerModel);
                if (UrlUtils.isRegistry(url)) {     // 检测 URL 协议是否为 registry，若是，说明用户想使用指定的注册中心
                    // 这里会将 map 中的参数整理成一个参数添加到 refer 参数中
                    urls.add(url.putAttribute(REFER_KEY, referenceParameters));      // 将map中的参数添加到url中
                } else {
                    URL peerUrl = getScopeModel().getApplicationModel().getBeanFactory()
                        .getBean(ClusterUtils.class)
                        .mergeUrl(url, referenceParameters);
                    peerUrl = peerUrl.putAttribute(PEER_KEY, true);
                    urls.add(peerUrl);
                }
            }
        }
    }

    /**
     * Get URLs from the registry and aggregate them.
     */
    private void aggregateUrlFromRegistry(Map<String, String> referenceParameters) {
        checkRegistry();
        List<URL> us = ConfigValidationUtils.loadRegistries(this, false);
        if (CollectionUtils.isNotEmpty(us)) {
            for (URL u : us) {
                URL monitorUrl = ConfigValidationUtils.loadMonitor(this, u);
                if (monitorUrl != null) {
                    u = u.putAttribute(MONITOR_KEY, monitorUrl);
                }
                u = u.setScopeModel(getScopeModel());
                u = u.setServiceModel(consumerModel);
                urls.add(u.putAttribute(REFER_KEY, referenceParameters));
            }
        }
        if (urls.isEmpty()) {
            throw new IllegalStateException(
                "No such any registry to reference " + interfaceName + " on the consumer " + NetUtils.getLocalHost() +
                    " use dubbo version " + Version.getVersion() +
                    ", please config <dubbo:registry address=\"...\" /> to your spring config.");
        }
    }


    /**
     * Make a remote reference, create a remote reference invoker
     * <p>
     * 通过 Protocol 的适配器选择对应的Protocol实现创建Invoker对象
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private void createInvokerForRemote() {
        if (urls.size() == 1) {   // 在单注册中心或是直连单个服务提供方的时候，通过 Protocol 的适配器选择对应的 Protocol 实现创建 Invoker 对象
            URL curUrl = urls.get(0);
            invoker = protocolSPI.refer(interfaceClass, curUrl);
            if (!UrlUtils.isRegistry(curUrl)) {
                List<Invoker<?>> invokers = new ArrayList<>();  // 多注册中心或是直连多个服务提供方的时候，会根据每个 URL 创建 Invoker 对象
                invokers.add(invoker);
                // 在单注册中心或是直连单个服务提供方的时候，通过Protocol的适配器选择对应的Protocol实现创建Invoker对象
                invoker = Cluster.getCluster(scopeModel, Cluster.DEFAULT).join(new StaticDirectory(curUrl, invokers), true);
            }
        } else {
            List<Invoker<?>> invokers = new ArrayList<>(); // 多注册中心或是直连多个服务提供方的时候，会根据每个 URL 创建 Invoker 对象
            URL registryUrl = null;
            for (URL url : urls) {
                // For multi-registry scenarios, it is not checked whether each referInvoker is available.
                // Because this invoker may become available later.
                invokers.add(protocolSPI.refer(interfaceClass, url));

                if (UrlUtils.isRegistry(url)) {     // 确定是多注册中心，还是直连多个 Provider
                    // use last registry url
                    registryUrl = url;
                }
            }

            if (registryUrl != null) {
                // registry url is available
                // for multi-subscription scenario, use 'zone-aware' policy by default
                // urls 集合中有多个注册中心，则使用 ZoneAwareCluster 作为 Cluster 的默认实现，生成对应的 Invoker 对象
                String cluster = registryUrl.getParameter(CLUSTER_KEY, ZoneAwareCluster.NAME);
                // The invoker wrap sequence would be: ZoneAwareClusterInvoker(StaticDirectory) -> FailoverClusterInvoker
                // (RegistryDirectory, routing happens here) -> Invoker
                invoker = Cluster.getCluster(registryUrl.getScopeModel(), cluster, false)
                    .join(new StaticDirectory(registryUrl, invokers), false);
            } else {
                // not a registry url, must be direct invoke.
                if (CollectionUtils.isEmpty(invokers)) {
                    throw new IllegalArgumentException("invokers == null");
                }
                URL curUrl = invokers.get(0).getUrl();
                String cluster = curUrl.getParameter(CLUSTER_KEY, Cluster.DEFAULT);
                // 多个Provider直连的场景中，使用Cluster适配器选择合适的扩展实现
                invoker = Cluster.getCluster(scopeModel, cluster).join(new StaticDirectory(curUrl, invokers), true);
            }
        }
    }

    private void checkInvokerAvailable() throws IllegalStateException {
        if (shouldCheck() && !invoker.isAvailable()) {        // 根据check配置决定是否检测Provider的可用性
            invoker.destroy();
            throw new IllegalStateException("Failed to check the status of the service "
                + interfaceName
                + ". No provider available for the service "
                + (group == null ? "" : group + "/")
                + interfaceName +
                (version == null ? "" : ":" + version)
                + " from the url "
                + invoker.getUrl()
                + " to the consumer "
                + NetUtils.getLocalHost() + " use dubbo version " + Version.getVersion());
        }
    }

    /**
     * This method should be called right after the creation of this class's instance, before any property in other config modules is used.
     * Check each config modules are created properly and override their properties if necessary.
     */
    protected void checkAndUpdateSubConfigs() {
        if (StringUtils.isEmpty(interfaceName)) {
            throw new IllegalStateException("<dubbo:reference interface=\"\" /> interface not allow null!");
        }

        // get consumer's global configuration
        completeCompoundConfigs();

        // init some null configuration.
        List<ConfigInitializer> configInitializers = this.getExtensionLoader(ConfigInitializer.class)
            .getActivateExtension(URL.valueOf("configInitializer://"), (String[]) null);
        configInitializers.forEach(e -> e.initReferConfig(this));

        if (getGeneric() == null && getConsumer() != null) {
            setGeneric(getConsumer().getGeneric());
        }
        if (ProtocolUtils.isGeneric(generic)) {
            if (interfaceClass != null && !interfaceClass.equals(GenericService.class)) {
                logger.warn(String.format("Found conflicting attributes for interface type: [interfaceClass=%s] and [generic=%s], " +
                        "because the 'generic' attribute has higher priority than 'interfaceClass', so change 'interfaceClass' to '%s'. " +
                        "Note: it will make this reference bean as a candidate bean of type '%s' instead of '%s' when resolving dependency in Spring.",
                    interfaceClass.getName(), generic, GenericService.class.getName(), GenericService.class.getName(), interfaceClass.getName()));
            }
            interfaceClass = GenericService.class;
        } else {
            try {
                if (getInterfaceClassLoader() != null && (interfaceClass == null || interfaceClass.getClassLoader() != getInterfaceClassLoader())) {
                    interfaceClass = Class.forName(interfaceName, true, getInterfaceClassLoader());
                } else if (interfaceClass == null) {
                    interfaceClass = Class.forName(interfaceName, true, Thread.currentThread()
                        .getContextClassLoader());
                }
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        }

        checkStubAndLocal(interfaceClass);
        ConfigValidationUtils.checkMock(interfaceClass, this);

        resolveFile();
        ConfigValidationUtils.validateReferenceConfig(this);
        postProcessConfig();
    }

    @Override
    protected void postProcessRefresh() {
        super.postProcessRefresh();
        checkAndUpdateSubConfigs();
    }

    protected void completeCompoundConfigs() {
        super.completeCompoundConfigs(consumer);
        if (consumer != null) {
            if (StringUtils.isEmpty(registryIds)) {
                setRegistryIds(consumer.getRegistryIds());
            }
        }
    }

    /**
     * Figure out should refer the service in the same JVM from configurations. The default behavior is true
     * 1. if injvm is specified, then use it
     * 2. then if a url is specified, then assume it's a remote call
     * 3. otherwise, check scope parameter
     * 4. if scope is not specified but the target service is provided in the same JVM, then prefer to make the local
     * call, which is the default behavior
     */
    protected boolean shouldJvmRefer(Map<String, String> map) {
        URL tmpUrl = new ServiceConfigURL("temp", "localhost", 0, map);
        boolean isJvmRefer;
        if (isInjvm() == null) {
            // if an url is specified, don't do local reference
            if (StringUtils.isNotEmpty(url)) {
                isJvmRefer = false;
            } else {
                // by default, reference local service if there is
                isJvmRefer = InjvmProtocol.getInjvmProtocol(getScopeModel()).isInjvmRefer(tmpUrl);
            }
        } else {
            isJvmRefer = isInjvm();
        }
        return isJvmRefer;
    }

    private void postProcessConfig() {
        List<ConfigPostProcessor> configPostProcessors = this.getExtensionLoader(ConfigPostProcessor.class)
            .getActivateExtension(URL.valueOf("configPostProcessor://"), (String[]) null);
        configPostProcessors.forEach(component -> component.postProcessReferConfig(this));
    }

    /**
     * just for test
     *
     * @return
     */
    @Deprecated
    public Invoker<?> getInvoker() {
        return invoker;
    }
}
