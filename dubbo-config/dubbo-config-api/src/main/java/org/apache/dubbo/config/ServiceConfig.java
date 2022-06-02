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
import org.apache.dubbo.common.URLBuilder;
import org.apache.dubbo.common.Version;
import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.threadpool.manager.ExecutorRepository;
import org.apache.dubbo.common.url.component.ServiceConfigURL;
import org.apache.dubbo.common.utils.ClassUtils;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.ConfigUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.config.annotation.Service;
import org.apache.dubbo.config.invoker.DelegateProviderMetaDataInvoker;
import org.apache.dubbo.config.support.Parameter;
import org.apache.dubbo.config.utils.ConfigValidationUtils;
import org.apache.dubbo.metadata.ServiceNameMapping;
import org.apache.dubbo.registry.client.metadata.MetadataUtils;
import org.apache.dubbo.rpc.*;
import org.apache.dubbo.rpc.cluster.ConfiguratorFactory;
import org.apache.dubbo.rpc.model.*;
import org.apache.dubbo.rpc.service.GenericService;

import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.dubbo.common.constants.CommonConstants.*;
import static org.apache.dubbo.common.constants.RegistryConstants.DYNAMIC_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.SERVICE_REGISTRY_PROTOCOL;
import static org.apache.dubbo.common.utils.NetUtils.*;
import static org.apache.dubbo.config.Constants.*;
import static org.apache.dubbo.registry.Constants.REGISTER_KEY;
import static org.apache.dubbo.remoting.Constants.BIND_IP_KEY;
import static org.apache.dubbo.remoting.Constants.BIND_PORT_KEY;
import static org.apache.dubbo.rpc.Constants.GENERIC_KEY;
import static org.apache.dubbo.rpc.Constants.*;
import static org.apache.dubbo.rpc.cluster.Constants.EXPORT_KEY;
import static org.apache.dubbo.rpc.support.ProtocolUtils.isGeneric;

public class ServiceConfig<T> extends ServiceConfigBase<T> {

    private static final long serialVersionUID = 7868244018230856253L;

    private static final Logger logger = LoggerFactory.getLogger(ServiceConfig.class);

    /**
     * A random port cache, the different protocols who have no port specified have different random port
     */
    private static final Map<String, Integer> RANDOM_PORT_MAP = new HashMap<String, Integer>();

    private Protocol protocolSPI;

    /**
     * A {@link ProxyFactory} implementation that will generate a exported service proxy,the JavassistProxyFactory is its
     * default implementation
     */
    private ProxyFactory proxyFactory;

    private ProviderModel providerModel;

    /**
     * Whether the provider has been exported
     */
    private transient volatile boolean exported;

    /**
     * The flag whether a service has unexported ,if the method unexported is invoked, the value is true
     */
    private transient volatile boolean unexported;

    private transient volatile AtomicBoolean initialized = new AtomicBoolean(false);

    /**
     * The exported services
     */
    private final List<Exporter<?>> exporters = new ArrayList<Exporter<?>>();

    private final List<ServiceListener> serviceListeners = new ArrayList<>();

    public ServiceConfig() {
    }

    public ServiceConfig(ModuleModel moduleModel) {
        super(moduleModel);
    }

    public ServiceConfig(Service service) {
        super(service);
    }

    public ServiceConfig(ModuleModel moduleModel, Service service) {
        super(moduleModel, service);
    }

    @Override
    protected void postProcessAfterScopeModelChanged(ScopeModel oldScopeModel, ScopeModel newScopeModel) {
        super.postProcessAfterScopeModelChanged(oldScopeModel, newScopeModel);
        protocolSPI = this.getExtensionLoader(Protocol.class).getAdaptiveExtension();
        proxyFactory = this.getExtensionLoader(ProxyFactory.class).getAdaptiveExtension();
    }

    @Override
    @Parameter(excluded = true, attribute = false)
    public boolean isExported() {
        return exported;
    }


    @Override
    @Parameter(excluded = true, attribute = false)
    public boolean isUnexported() {
        return unexported;
    }

    @Override
    public void unexport() {
        if (!exported) {
            return;
        }
        if (unexported) {
            return;
        }
        if (!exporters.isEmpty()) {
            for (Exporter<?> exporter : exporters) {
                try {
                    exporter.unexport();
                } catch (Throwable t) {
                    logger.warn("Unexpected error occured when unexport " + exporter, t);
                }
            }
            exporters.clear();
        }
        unexported = true;
        onUnexpoted();
        ModuleServiceRepository repository = getScopeModel().getServiceRepository();
        repository.unregisterProvider(providerModel);
    }

    /**
     * for early init serviceMetadata
     */
    public void init() {
        if (this.initialized.compareAndSet(false, true)) {
            // load ServiceListeners from extension
            // 使用 Dubbo SPI 机制获取对应的 ServiceDiscovery 对象
            ExtensionLoader<ServiceListener> extensionLoader = this.getExtensionLoader(ServiceListener.class);
            this.serviceListeners.addAll(extensionLoader.getSupportedExtensionInstances());
        }
        initServiceMetadata(provider);
        serviceMetadata.setServiceType(getInterfaceClass());
        serviceMetadata.setTarget(getRef());
        serviceMetadata.generateServiceKey();
    }

    @Override
    public void export() {
        if (this.exported) {
            return;
        }

        // ensure start module, compatible with old api usage
        getScopeModel().getDeployer().start();

        synchronized (this) {
            if (this.exported) {
                return;
            }

            if (!this.isRefreshed()) {
                this.refresh();
            }
            if (this.shouldExport()) {
                this.init();

                if (shouldDelay()) {        // 延迟发布
                    doDelayExport();
                } else {
                    doExport();      // 立即发布
                }
            }
        }
    }

    protected void doDelayExport() {
        getScopeModel().getDefaultExtension(ExecutorRepository.class).getServiceExportExecutor()
            .schedule(() -> {
                try {
                    doExport();
                } catch (Exception e) {
                    logger.error("Failed to export service config: " + interfaceName, e);
                }
            }, getDelay(), TimeUnit.MILLISECONDS);
    }

    /**
     * 回调监听器
     */
    protected void exported() {
        exported = true;
        List<URL> exportedURLs = this.getExportedUrls();
        exportedURLs.forEach(url -> {
            if (url.getParameters().containsKey(SERVICE_NAME_MAPPING_KEY)) {
                ServiceNameMapping serviceNameMapping = ServiceNameMapping.getDefaultExtension(getScopeModel());
                try {
                    boolean succeeded = serviceNameMapping.map(url);
                    if (succeeded) {
                        logger.info("Successfully registered interface application mapping for service " + url.getServiceKey());
                    } else {
                        logger.error("Failed register interface application mapping for service " + url.getServiceKey());
                    }
                } catch (Exception e) {
                    logger.error("Failed register interface application mapping for service " + url.getServiceKey(), e);
                }
            }
        });
        onExported();
    }

    /**
     * 检查并更新各项配置。
     * <p>
     * 检查各项配置是否合理，并补齐一些缺省的配置信息
     */
    private void checkAndUpdateSubConfigs() {

        // Use default configs defined explicitly with global scope
        completeCompoundConfigs();

        checkProtocol();

        // init some null configuration.
        List<ConfigInitializer> configInitializers = this.getExtensionLoader(ConfigInitializer.class)
            .getActivateExtension(URL.valueOf("configInitializer://", getScopeModel()), (String[]) null);
        configInitializers.forEach(e -> e.initServiceConfig(this));

        // if protocol is not injvm checkRegistry
        if (!isOnlyInJvm()) {
            checkRegistry();
        }

        if (StringUtils.isEmpty(interfaceName)) {
            throw new IllegalStateException("<dubbo:service interface=\"\" /> interface not allow null!");
        }

        if (ref instanceof GenericService) {
            interfaceClass = GenericService.class;
            if (StringUtils.isEmpty(generic)) {
                generic = Boolean.TRUE.toString();
            }
        } else {
            try {
                if (getInterfaceClassLoader() != null) {
                    interfaceClass = Class.forName(interfaceName, true, getInterfaceClassLoader());
                } else {
                    interfaceClass = Class.forName(interfaceName, true, Thread.currentThread().getContextClassLoader());
                }
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
            checkRef();
            generic = Boolean.FALSE.toString();
        }
        if (local != null) {
            if ("true".equals(local)) {
                local = interfaceName + "Local";
            }
            Class<?> localClass;
            try {
                localClass = ClassUtils.forNameWithThreadContextClassLoader(local);
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
            if (!interfaceClass.isAssignableFrom(localClass)) {
                throw new IllegalStateException("The local implementation class " + localClass.getName() + " not implement interface " + interfaceName);
            }
        }
        if (stub != null) {
            if ("true".equals(stub)) {
                stub = interfaceName + "Stub";
            }
            Class<?> stubClass;
            try {
                stubClass = ClassUtils.forNameWithThreadContextClassLoader(stub);
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
            if (!interfaceClass.isAssignableFrom(stubClass)) {
                throw new IllegalStateException("The stub implementation class " + stubClass.getName() + " not implement interface " + interfaceName);
            }
        }
        checkStubAndLocal(interfaceClass);
        ConfigValidationUtils.checkMock(interfaceClass, this);
        ConfigValidationUtils.validateServiceConfig(this);
        postProcessConfig();
    }

    /**
     * 初始化前置处理
     */
    @Override
    protected void postProcessRefresh() {
        super.postProcessRefresh();          // 初始化元数据相关服务
        checkAndUpdateSubConfigs();          // 检查并更新各项配置
    }

    protected synchronized void doExport() {
        if (unexported) {
            throw new IllegalStateException("The service " + interfaceClass.getName() + " has already unexported!");
        }
        if (exported) {
            return;
        }

        if (StringUtils.isEmpty(path)) {
            path = interfaceName;
        }
        doExportUrls(); // 加载注册中心信息
        exported();     // 回调监听器
    }

    /**
     * 加载注册中心信息，即将 RegistryConfig 配置解析成 registryUrl。
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private void doExportUrls() {
        ModuleServiceRepository repository = getScopeModel().getServiceRepository();
        ServiceDescriptor serviceDescriptor;
        final boolean serverService = ref instanceof ServerService;
        if (serverService) {
            serviceDescriptor = ((ServerService) ref).getServiceDescriptor();
            repository.registerService(serviceDescriptor);
        } else {
            serviceDescriptor = repository.registerService(getInterfaceClass());
        }
        providerModel = new ProviderModel(getUniqueServiceName(),
            ref,
            serviceDescriptor,
            this,
            getScopeModel(),
            serviceMetadata);

        repository.registerProvider(providerModel);

        List<URL> registryURLs = ConfigValidationUtils.loadRegistries(this, true);  // 加载注册中心信息

        for (ProtocolConfig protocolConfig : protocols) {                   // 在每个注册中心发布服务。
            String pathKey = URL.buildKey(getContextPath(protocolConfig)
                .map(p -> p + "/" + path)
                .orElse(path), group, version);
            // stub service will use generated service name
            if (!serverService) {
                // In case user specified path, register service one more time to map it to path.
                repository.registerService(pathKey, interfaceClass);
            }
            doExportUrlsFor1Protocol(protocolConfig, registryURLs);
        }
    }

    /**
     * 发布一个服务的入口，也是规定服务发布流程的地方，其中涉及 Provider URL 的组装、本地服务发布流程以及远程服务发布流程。
     *
     * @param protocolConfig 一个服务接口可以以多种协议进行发布，每种协议都对应一个 ProtocolConfig
     */
    private void doExportUrlsFor1Protocol(ProtocolConfig protocolConfig, List<URL> registryURLs) {
        Map<String, String> map = buildAttributes(protocolConfig);      // 设置服务 URL 中的参数

        // remove null key and null value
        map.keySet().removeIf(key -> key == null || map.get(key) == null);
        // init serviceMetadata attachments
        serviceMetadata.getAttachments().putAll(map);

        URL url = buildUrl(protocolConfig, map);        // 组装 URL

        exportUrl(url, registryURLs);
    }

    /**
     * 从 MetricsConfig、ApplicationConfig、ModuleConfig、ProviderConfig、ProtocolConfig 中获取配置信息，并作为参数添加到 URL 中。
     */
    private Map<String, String> buildAttributes(ProtocolConfig protocolConfig) {

        Map<String, String> map = new HashMap<>();      // 配置信息也是记录到 Map 集合中，后续作为 URL 参数。
        map.put(SIDE_KEY, PROVIDER_SIDE);

        // append params with basic configs,
        // 添加URL参数，例如Dubbo版本、时间戳、当前PID等
        ServiceConfig.appendRuntimeParameters(map);
        // 下面会从各个Config获取参数，例如，application、interface参数等
        AbstractConfig.appendParameters(map, getApplication());
        AbstractConfig.appendParameters(map, getModule());
        // remove 'default.' prefix for configs from ProviderConfig
        // appendParameters(map, provider, Constants.DEFAULT_KEY);
        AbstractConfig.appendParameters(map, provider);
        AbstractConfig.appendParameters(map, protocolConfig);
        AbstractConfig.appendParameters(map, this);
        appendMetricsCompatible(map);

        // append params with method configs,
        if (CollectionUtils.isNotEmpty(getMethods())) {
            getMethods().forEach(method -> appendParametersWithMethod(method, map));
        }

        if (isGeneric(generic)) {       // 泛化调用还是普通调用，向 Map 集合中添加不同的键值对。
            map.put(GENERIC_KEY, generic);
            map.put(METHODS_KEY, ANY_VALUE);
        } else {
            String revision = Version.getVersion(interfaceClass, version);
            if (revision != null && revision.length() > 0) {
                map.put(REVISION_KEY, revision);
            }

            String[] methods = methods(interfaceClass);
            if (methods.length == 0) {
                logger.warn("No method found in service interface " + interfaceClass.getName());
                map.put(METHODS_KEY, ANY_VALUE);
            } else {
                map.put(METHODS_KEY, StringUtils.join(new HashSet<>(Arrays.asList(methods)), ","));
            }
        }

        /*
         * 获取 token 配置，并添加到 Map 集合中，默认随机生成 UUID。
         *
         * Here the token value configured by the provider is used to assign the value to ServiceConfig#token
         */
        if (ConfigUtils.isEmpty(token) && provider != null) {
            token = provider.getToken();
        }

        if (!ConfigUtils.isEmpty(token)) {
            if (ConfigUtils.isDefault(token)) {
                map.put(TOKEN_KEY, UUID.randomUUID().toString());    // 获取 token 配置，并添加到 Map 集合中，默认随机生成 UUID。
            } else {
                map.put(TOKEN_KEY, token);
            }
        }

        if (ref instanceof ServerService) {
            map.put(PROXY_KEY, CommonConstants.NATIVE_STUB);
        }

        return map;
    }

    /**
     * 解析指定方法的 MethodConfig 配置以及方法参数的 ArgumentConfig 配置，得到的配置信息也是记录到 Map 集合中，后续作为 URL 参数。
     */
    private void appendParametersWithMethod(MethodConfig method, Map<String, String> params) {
        AbstractConfig.appendParameters(params, method, method.getName());

        String retryKey = method.getName() + ".retry";
        if (params.containsKey(retryKey)) {
            String retryValue = params.remove(retryKey);
            if ("false".equals(retryValue)) {
                params.put(method.getName() + ".retries", "0");
            }
        }

        List<ArgumentConfig> arguments = method.getArguments();
        if (CollectionUtils.isNotEmpty(arguments)) {
            Method matchedMethod = findMatchedMethod(method);
            if (matchedMethod != null) {
                arguments.forEach(argument -> appendArgumentConfig(argument, matchedMethod, params));
            }
        }
    }

    private Method findMatchedMethod(MethodConfig methodConfig) {
        for (Method method : interfaceClass.getMethods()) {
            if (method.getName().equals(methodConfig.getName())) {
                return method;
            }
        }
        return null;
    }

    private void appendArgumentConfig(ArgumentConfig argument, Method method, Map<String, String> params) {
        if (StringUtils.isNotEmpty(argument.getType())) {
            Integer index = findArgumentIndexIndexWithGivenType(argument, method);
            AbstractConfig.appendParameters(params, argument, method.getName() + "." + index);
        } else if (hasIndex(argument)) {
            AbstractConfig.appendParameters(params, argument, method.getName() + "." + argument.getIndex());
        } else {
            throw new IllegalArgumentException("Argument config must set index or type attribute.eg: <dubbo:argument index='0' .../> or <dubbo:argument type=xxx .../>");
        }
    }

    private boolean hasIndex(ArgumentConfig argument) {
        return argument.getIndex() != -1;
    }

    private boolean isTypeMatched(String type, Integer index, Class<?>[] argtypes) {
        return index != null && index >= 0 && index < argtypes.length && argtypes[index].getName().equals(type);
    }

    private Integer findArgumentIndexIndexWithGivenType(ArgumentConfig argument, Method method) {
        Class<?>[] argTypes = method.getParameterTypes();
        // one callback in the method
        if (hasIndex(argument)) {
            Integer index = argument.getIndex();
            String type = argument.getType();
            if (isTypeMatched(type, index, argTypes)) {
                return index;
            } else {
                throw new IllegalArgumentException("Argument config error : the index attribute and type attribute not match :index :" + argument.getIndex() + ", type:" + argument.getType());
            }
        } else {
            // multiple callbacks in the method
            for (int j = 0; j < argTypes.length; j++) {
                if (isTypeMatched(argument.getType(), j, argTypes)) {
                    return j;
                }
            }
            throw new IllegalArgumentException("Argument config error : no argument matched with the type:" + argument.getType());
        }
    }

    /**
     * 组装 URL 的核心实现
     */
    private URL buildUrl(ProtocolConfig protocolConfig, Map<String, String> params) {
        String name = protocolConfig.getName();     // 获取协议名称
        if (StringUtils.isEmpty(name)) {            // 默认使用 Dubbo 协议
            name = DUBBO;
        }

        // export service
        // 获取 host、port 值，并开始组装服务的 URL。
        String host = findConfiguredHosts(protocolConfig, provider, params);
        Integer port = findConfiguredPort(protocolConfig, provider, this.getExtensionLoader(Protocol.class), name, params);
        URL url = new ServiceConfigURL(name, null, null, host, port, getContextPath(protocolConfig).map(p -> p + "/" + path).orElse(path), params);

        // You can customize Configurator to append extra parameters
        // 根据 Configurator 覆盖或新增 URL 参数。
        if (this.getExtensionLoader(ConfiguratorFactory.class)
            .hasExtension(url.getProtocol())) {
            url = this.getExtensionLoader(ConfiguratorFactory.class)
                .getExtension(url.getProtocol()).getConfigurator(url).configure(url);
        }
        url = url.setScopeModel(getScopeModel());
        url = url.setServiceModel(providerModel);
        return url;
    }

    /**
     * 服务发布入口
     */
    private void exportUrl(URL url, List<URL> registryURLs) {
        // 从URL中获取scope参数，其中可选值有none、remote、local三个， 分别代表不发布、发布到本地以及发布到远端，具体含义在下面一一介绍
        String scope = url.getParameter(SCOPE_KEY);
        // don't export when none is configured
        if (!SCOPE_NONE.equalsIgnoreCase(scope)) {          // scope 不为 none，才进行发布

            // export to local if the config is not remote (export to remote only when config is remote)
            if (!SCOPE_REMOTE.equalsIgnoreCase(scope)) {    // 发布到本地
                exportLocal(url);
            }

            // export to remote if the config is not local (export to local only when config is local)
            if (!SCOPE_LOCAL.equalsIgnoreCase(scope)) {     // 发布到远端的注册中心
                url = exportRemote(url, registryURLs);      // 发布到远端的注册中心
                if (!isGeneric(generic) && !getScopeModel().isInternal()) {
                    MetadataUtils.publishServiceDefinition(url, providerModel.getServiceModel(), getApplicationModel());
                }
            }
        }
        this.urls.add(url);
    }

    /**
     * 远程发布
     */
    private URL exportRemote(URL url, List<URL> registryURLs) {
        if (CollectionUtils.isNotEmpty(registryURLs)) {
            for (URL registryURL : registryURLs) {      // 当前配置了至少一个注册中心
                if (SERVICE_REGISTRY_PROTOCOL.equals(registryURL.getProtocol())) {
                    url = url.addParameterIfAbsent(SERVICE_NAME_MAPPING_KEY, "true");
                }

                // if protocol is only injvm, not register
                // injvm 协议只在 exportLocal() 中有用，不会将服务发布到注册中心 所以这里忽略 injvm 协议
                if (LOCAL_PROTOCOL.equalsIgnoreCase(url.getProtocol())) {
                    continue;
                }

                // 设置服务URL的dynamic参数
                url = url.addParameterIfAbsent(DYNAMIC_KEY, registryURL.getParameter(DYNAMIC_KEY));
                // 创建 monitorUrl，并作为 monitor 参数添加到服务 URL 中
                URL monitorUrl = ConfigValidationUtils.loadMonitor(this, registryURL);
                if (monitorUrl != null) {
                    url = url.putAttribute(MONITOR_KEY, monitorUrl);
                }

                // For providers, this is used to enable custom proxy to generate invoker
                // 设置服务 URL 的 proxy 参数，即生成动态代理方式(jdk 或是 javassist)，作为参数添加到 RegistryURL 中
                String proxy = url.getParameter(PROXY_KEY);
                if (StringUtils.isNotEmpty(proxy)) {
                    registryURL = registryURL.addParameter(PROXY_KEY, proxy);
                }

                if (logger.isInfoEnabled()) {
                    if (url.getParameter(REGISTER_KEY, true)) {
                        logger.info("Register dubbo service " + interfaceClass.getName() + " url "
                            + url + " to registry " + registryURL.getAddress());
                    } else {
                        logger.info("Export dubbo service " + interfaceClass.getName() + " to url " + url);
                    }
                }

                // 发布到注册中心
                doExportUrl(registryURL.putAttribute(EXPORT_KEY, url), true);
            }

        } else {

            if (logger.isInfoEnabled()) {
                logger.info("Export dubbo service " + interfaceClass.getName() + " to url " + url);
            }

            doExportUrl(url, true);     // 不存在注册中心，仅发布服务
        }


        return url;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void doExportUrl(URL url, boolean withMetaData) {
        // 为服务实现类的对象创建相应的Invoker，getInvoker()方法的第三个参数中，会将服务URL作为export参数添加到RegistryURL中
        // 这里的PROXY_FACTORY是ProxyFactory接口的适配器
        Invoker<?> invoker = proxyFactory.getInvoker(ref, (Class) interfaceClass, url);
        if (withMetaData) {
            // DelegateProviderMetaDataInvoker 是个装饰类，将当前 ServiceConfig 和 Invoker 关联起来而已，invoke() 方法透传给底层 Invoker 对象
            invoker = new DelegateProviderMetaDataInvoker(invoker, this);
        }
        // 调用 Protocol 实现，进行发布这里的 PROTOCOL 是 Protocol 接口的适配器
        Exporter<?> exporter = protocolSPI.export(invoker);
        exporters.add(exporter);
    }


    /**
     * 本地发布，走 TCP/IP 协议栈，不走网卡
     *
     * always export injvm
     */
    private void exportLocal(URL url) {
        URL local = URLBuilder.from(url)        // 创建新URL
            .setProtocol(LOCAL_PROTOCOL)
            .setHost(LOCALHOST_VALUE)
            .setPort(0)
            .build();
        local = local.setScopeModel(getScopeModel())          // 本地发布
            .setServiceModel(providerModel);
        doExportUrl(local, false);
        logger.info("Export dubbo service " + interfaceClass.getName() + " to local registry url : " + local);
    }

    /**
     * Determine if it is injvm
     *
     * @return
     */
    private boolean isOnlyInJvm() {
        return getProtocols().size() == 1
            && LOCAL_PROTOCOL.equalsIgnoreCase(getProtocols().get(0).getName());
    }

    private void postProcessConfig() {
        List<ConfigPostProcessor> configPostProcessors = this.getExtensionLoader(ConfigPostProcessor.class)
            .getActivateExtension(URL.valueOf("configPostProcessor://", getScopeModel()), (String[]) null);
        configPostProcessors.forEach(component -> component.postProcessServiceConfig(this));
    }

    public void addServiceListener(ServiceListener listener) {
        this.serviceListeners.add(listener);
    }

    protected void onExported() {
        for (ServiceListener serviceListener : this.serviceListeners) {
            serviceListener.exported(this);
        }
    }

    protected void onUnexpoted() {
        for (ServiceListener serviceListener : this.serviceListeners) {
            serviceListener.unexported(this);
        }
    }

    /**
     * Register & bind IP address for service provider, can be configured separately.
     * Configuration priority: environment variables -> java system properties -> host property in config file ->
     * /etc/hosts -> default network address -> first available network address
     *
     * @param protocolConfig
     * @param map
     * @return
     */
    private static String findConfiguredHosts(ProtocolConfig protocolConfig,
                                              ProviderConfig provider,
                                              Map<String, String> map) {
        boolean anyhost = false;

        String hostToBind = getValueFromConfig(protocolConfig, DUBBO_IP_TO_BIND);
        if (hostToBind != null && hostToBind.length() > 0 && isInvalidLocalHost(hostToBind)) {
            throw new IllegalArgumentException("Specified invalid bind ip from property:" + DUBBO_IP_TO_BIND + ", value:" + hostToBind);
        }

        // if bind ip is not found in environment, keep looking up
        if (StringUtils.isEmpty(hostToBind)) {
            hostToBind = protocolConfig.getHost();
            if (provider != null && StringUtils.isEmpty(hostToBind)) {
                hostToBind = provider.getHost();
            }
            if (isInvalidLocalHost(hostToBind)) {
                anyhost = true;
                if (logger.isDebugEnabled()) {
                    logger.info("No valid ip found from environment, try to get local host.");
                }
                hostToBind = getLocalHost();
            }
        }

        map.put(BIND_IP_KEY, hostToBind);

        // registry ip is not used for bind ip by default
        String hostToRegistry = getValueFromConfig(protocolConfig, DUBBO_IP_TO_REGISTRY);
        if (StringUtils.isNotEmpty(hostToRegistry) && isInvalidLocalHost(hostToRegistry)) {
            throw new IllegalArgumentException("Specified invalid registry ip from property:" + DUBBO_IP_TO_REGISTRY + ", value:" + hostToRegistry);
        } else if (StringUtils.isEmpty(hostToRegistry)) {
            // bind ip is used as registry ip by default
            hostToRegistry = hostToBind;
        }

        map.put(ANYHOST_KEY, String.valueOf(anyhost));

        return hostToRegistry;
    }


    /**
     * Register port and bind port for the provider, can be configured separately
     * Configuration priority: environment variable -> java system properties -> port property in protocol config file
     * -> protocol default port
     *
     * @param protocolConfig
     * @param name
     * @return
     */
    private static synchronized Integer findConfiguredPort(ProtocolConfig protocolConfig,
                                                           ProviderConfig provider,
                                                           ExtensionLoader<Protocol> extensionLoader,
                                                           String name, Map<String, String> map) {
        Integer portToBind;

        // parse bind port from environment
        String port = getValueFromConfig(protocolConfig, DUBBO_PORT_TO_BIND);
        portToBind = parsePort(port);

        // if there's no bind port found from environment, keep looking up.
        if (portToBind == null) {
            portToBind = protocolConfig.getPort();
            if (provider != null && (portToBind == null || portToBind == 0)) {
                portToBind = provider.getPort();
            }
            final int defaultPort = extensionLoader.getExtension(name).getDefaultPort();
            if (portToBind == null || portToBind == 0) {
                portToBind = defaultPort;
            }
            if (portToBind <= 0) {
                portToBind = getRandomPort(name);
                if (portToBind == null || portToBind < 0) {
                    portToBind = getAvailablePort(defaultPort);
                    putRandomPort(name, portToBind);
                }
            }
        }

        // save bind port, used as url's key later
        map.put(BIND_PORT_KEY, String.valueOf(portToBind));

        // registry port, not used as bind port by default
        String portToRegistryStr = getValueFromConfig(protocolConfig, DUBBO_PORT_TO_REGISTRY);
        Integer portToRegistry = parsePort(portToRegistryStr);
        if (portToRegistry == null) {
            portToRegistry = portToBind;
        }

        return portToRegistry;
    }

    private static Integer parsePort(String configPort) {
        Integer port = null;
        if (StringUtils.isNotEmpty(configPort)) {
            try {
                int intPort = Integer.parseInt(configPort);
                if (isInvalidPort(intPort)) {
                    throw new IllegalArgumentException("Specified invalid port from env value:" + configPort);
                }
                port = intPort;
            } catch (Exception e) {
                throw new IllegalArgumentException("Specified invalid port from env value:" + configPort);
            }
        }
        return port;
    }

    private static String getValueFromConfig(ProtocolConfig protocolConfig, String key) {
        String protocolPrefix = protocolConfig.getName().toUpperCase() + "_";
        String value = ConfigUtils.getSystemProperty(protocolPrefix + key);
        if (StringUtils.isEmpty(value)) {
            value = ConfigUtils.getSystemProperty(key);
        }
        return value;
    }

    private static Integer getRandomPort(String protocol) {
        protocol = protocol.toLowerCase();
        return RANDOM_PORT_MAP.getOrDefault(protocol, Integer.MIN_VALUE);
    }

    private static void putRandomPort(String protocol, Integer port) {
        protocol = protocol.toLowerCase();
        if (!RANDOM_PORT_MAP.containsKey(protocol)) {
            RANDOM_PORT_MAP.put(protocol, port);
            logger.warn("Use random available port(" + port + ") for protocol " + protocol);
        }
    }

}
