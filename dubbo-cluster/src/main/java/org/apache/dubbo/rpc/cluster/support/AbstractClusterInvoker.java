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
package org.apache.dubbo.rpc.cluster.support;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.Version;
import org.apache.dubbo.common.config.Configuration;
import org.apache.dubbo.common.config.ConfigurationUtils;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.profiler.ProfilerSwitch;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.InvocationProfilerUtils;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.ClusterInvoker;
import org.apache.dubbo.rpc.cluster.Directory;
import org.apache.dubbo.rpc.cluster.LoadBalance;
import org.apache.dubbo.rpc.model.ApplicationModel;
import org.apache.dubbo.rpc.model.ScopeModelUtil;
import org.apache.dubbo.rpc.support.RpcUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_LOADBALANCE;
import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_RESELECT_COUNT;
import static org.apache.dubbo.common.constants.CommonConstants.ENABLE_CONNECTIVITY_VALIDATION;
import static org.apache.dubbo.common.constants.CommonConstants.LOADBALANCE_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.RESELECT_COUNT;
import static org.apache.dubbo.rpc.cluster.Constants.CLUSTER_AVAILABLE_CHECK_KEY;
import static org.apache.dubbo.rpc.cluster.Constants.CLUSTER_STICKY_KEY;
import static org.apache.dubbo.rpc.cluster.Constants.DEFAULT_CLUSTER_AVAILABLE_CHECK;
import static org.apache.dubbo.rpc.cluster.Constants.DEFAULT_CLUSTER_STICKY;

/**
 * 一个是实现的 Invoker 接口，对 Invoker.invoke() 方法进行通用的抽象实现；另一个是实现通用的负载均衡算法。
 *
 * 1.当调用进入 Cluster 的时候，Cluster 会创建一个 AbstractClusterInvoker 对象，在这个 AbstractClusterInvoker 中，首先会从 Directory 中获取当前 Invoker 集合；
 * 2.然后按照 Router 集合进行路由，得到符合条件的 Invoker 集合；
 * 3.接下来按照 LoadBalance 指定的负载均衡策略得到最终要调用的 Invoker 对象。
 */
public abstract class AbstractClusterInvoker<T> implements ClusterInvoker<T> {

    private static final Logger logger = LoggerFactory.getLogger(AbstractClusterInvoker.class);

    protected Directory<T> directory;

    protected boolean availableCheck;

    private volatile int reselectCount = DEFAULT_RESELECT_COUNT;

    private volatile boolean enableConnectivityValidation = true;

    private AtomicBoolean destroyed = new AtomicBoolean(false);

    private volatile Invoker<T> stickyInvoker = null;

    public AbstractClusterInvoker() {
    }

    public AbstractClusterInvoker(Directory<T> directory) {
        this(directory, directory.getUrl());
    }

    public AbstractClusterInvoker(Directory<T> directory, URL url) {
        if (directory == null) {
            throw new IllegalArgumentException("service directory == null");
        }

        this.directory = directory;
        //sticky: invoker.isAvailable() should always be checked before using when availablecheck is true.
        this.availableCheck = url.getParameter(CLUSTER_AVAILABLE_CHECK_KEY, DEFAULT_CLUSTER_AVAILABLE_CHECK);
        Configuration configuration = ConfigurationUtils.getGlobalConfiguration(url.getOrDefaultModuleModel());
        this.reselectCount = configuration.getInt(RESELECT_COUNT, DEFAULT_RESELECT_COUNT);
        this.enableConnectivityValidation = configuration.getBoolean(ENABLE_CONNECTIVITY_VALIDATION, true);
    }

    @Override
    public Class<T> getInterface() {
        return getDirectory().getInterface();
    }

    @Override
    public URL getUrl() {
        return getDirectory().getConsumerUrl();
    }

    @Override
    public URL getRegistryUrl() {
        return getDirectory().getUrl();
    }

    @Override
    public boolean isAvailable() {
        Invoker<T> invoker = stickyInvoker;
        if (invoker != null) {
            return invoker.isAvailable();
        }
        return getDirectory().isAvailable();
    }

    @Override
    public Directory<T> getDirectory() {
        return directory;
    }

    @Override
    public void destroy() {
        if (destroyed.compareAndSet(false, true)) {
            getDirectory().destroy();
        }
    }

    @Override
    public boolean isDestroyed() {
        return destroyed.get();
    }

    /**
     * 根据配置决定是否开启粘滞连接特性，如果开启了，则需要将上次使用的 Invoker 缓存起来，只要 Provider 节点可用就直接调用，不会再进行负载均衡。
     * 如果调用失败，才会重新进行负载均衡，并且排除已经重试过的 Provider 节点。
     *
     * Select a invoker using loadbalance policy.</br>
     * a) Firstly, select an invoker using loadbalance. If this invoker is in previously selected list, or,
     * if this invoker is unavailable, then continue step b (reselect), otherwise return the first selected invoker</br>
     * <p>
     * b) Reselection, the validation rule for reselection: selected > available. This rule guarantees that
     * the selected invoker has the minimum chance to be one in the previously selected list, and also
     * guarantees this invoker is available.
     *
     * @param loadbalance 此次使用的 LoadBalance 实现
     * @param invocation  此次服务调用的上下文信息
     * @param invokers    待选择的 Invoker 集合
     * @param selected    用来记录负载均衡已经选出来、尝试过的 Invoker 集合
     * @return the invoker which will final to do invoke.
     * @throws RpcException exception
     */
    protected Invoker<T> select(LoadBalance loadbalance, Invocation invocation,
                                List<Invoker<T>> invokers, List<Invoker<T>> selected) throws RpcException {

        if (CollectionUtils.isEmpty(invokers)) {
            return null;
        }
        String methodName = invocation == null ? StringUtils.EMPTY_STRING : invocation.getMethodName();    // 获取调用方法名

        // 获取sticky配置，sticky表示粘滞连接，所谓粘滞连接是指Consumer会尽可能地调用同一个Provider节点，除非这个Provider无法提供服务
        boolean sticky = invokers.get(0).getUrl()
            .getMethodParameter(methodName, CLUSTER_STICKY_KEY, DEFAULT_CLUSTER_STICKY);

        //ignore overloaded method
        // 检测invokers列表是否包含sticky Invoker，如果不包含， 说明stickyInvoker代表的服务提供者挂了，此时需要将其置空
        if (stickyInvoker != null && !invokers.contains(stickyInvoker)) {
            stickyInvoker = null;
        }
        //ignore concurrency problem
        // 如果开启了粘滞连接特性，需要先判断这个Provider节点是否已经重试过了
        if (sticky && stickyInvoker != null     // 表示粘滞连接
            && (selected == null || !selected.contains(stickyInvoker))) {       // 表示 stickyInvoker 未重试过
            if (availableCheck && stickyInvoker.isAvailable()) {   // 检测当前 stickyInvoker 是否可用，如果可用，直接返回 stickyInvoker
                return stickyInvoker;
            }
        }

        // 执行到这里，说明前面的 stickyInvoker 为空，或者不可用这里会继续调用 doSelect 选择新的 Invoker 对象
        Invoker<T> invoker = doSelect(loadbalance, invocation, invokers, selected);

        if (sticky) {       // 是否开启粘滞，更新 stickyInvoker 字段
            stickyInvoker = invoker;
        }

        return invoker;
    }

    /**
     * 通过 LoadBalance 选择 Invoker 对象；如果选出来的 Invoker 不稳定或不可用，会调用 reselect() 方法进行重选。
     */
    private Invoker<T> doSelect(LoadBalance loadbalance, Invocation invocation,
                                List<Invoker<T>> invokers, List<Invoker<T>> selected) throws RpcException {

        if (CollectionUtils.isEmpty(invokers)) {            // 判断是否需要进行负载均衡，Invoker集合为空，直接返回null
            return null;
        }
        if (invokers.size() == 1) {                     // 只有一个Invoker对象，直接返回即可
            Invoker<T> tInvoker = invokers.get(0);
            checkShouldInvalidateInvoker(tInvoker);
            return tInvoker;
        }
        Invoker<T> invoker = loadbalance.select(invokers, getUrl(), invocation);    // 通过LoadBalance实现选择Invoker对象

        //If the `invoker` is in the  `selected` or invoker is unavailable && availablecheck is true, reselect.
        boolean isSelected = selected != null && selected.contains(invoker);
        boolean isUnavailable = availableCheck && !invoker.isAvailable() && getUrl() != null;

        // 如果 LoadBalance 选出的 Invoker 对象，已经尝试过请求了或不可用，则需要调用 reselect() 方法重选
        if (isUnavailable) {
            invalidateInvoker(invoker);
        }
        if (isSelected || isUnavailable) {
            try {
                Invoker<T> rInvoker = reselect(loadbalance, invocation, invokers, selected, availableCheck);  // 调用reselect()方法重选
                if (rInvoker != null) {           // 如果重选的 Invoker 对象不为空，则直接返回这个 rInvoker
                    invoker = rInvoker;
                } else {
                    //Check the index of current selected invoker, if it's not the last one, choose the one at index+1.
                    int index = invokers.indexOf(invoker);
                    try {
                        //Avoid collision
                        invoker = invokers.get((index + 1) % invokers.size()); // 如果重选的 Invoker 对象为空，则返回该 Invoker 的下一个 Invoker 对象
                    } catch (Exception e) {
                        logger.warn(e.getMessage() + " may because invokers list dynamic change, ignore.", e);
                    }
                }
            } catch (Throwable t) {
                logger.error("cluster reselect fail reason is :" + t.getMessage() + " if can not solve, you can set cluster.availablecheck=false in url", t);
            }
        }

        return invoker;
    }

    /**
     * 重新进行一次负载均衡。
     *
     * 1.首先对未尝试过的可用 Invokers 进行负载均衡，
     * 2.如果已经全部重试过了，则将尝试过的 Provider 节点过滤掉，然后在可用的 Provider 节点中重新进行负载均衡。
     *
     * Reselect, use invokers not in `selected` first, if all invokers are in `selected`,
     * just pick an available one using loadbalance policy.
     *
     * @param loadbalance    load balance policy
     * @param invocation     invocation
     * @param invokers       invoker candidates
     * @param selected       exclude selected invokers or not
     * @param availableCheck check invoker available if true
     * @return the reselect result to do invoke
     * @throws RpcException exception
     */
    private Invoker<T> reselect(LoadBalance loadbalance, Invocation invocation,
                                List<Invoker<T>> invokers, List<Invoker<T>> selected, boolean availableCheck) throws RpcException {

        // Allocating one in advance, this list is certain to be used.
        // 用于记录要重新进行负载均衡的 Invoker 集合
        List<Invoker<T>> reselectInvokers = new ArrayList<>(Math.min(invokers.size(), reselectCount));

        // 1. Try picking some invokers not in `selected`.
        //    1.1. If all selectable invokers' size is smaller than reselectCount, just add all
        //    1.2. If all selectable invokers' size is greater than reselectCount, randomly select reselectCount.
        //            The result size of invokers might smaller than reselectCount due to disAvailable or de-duplication (might be zero).
        //            This means there is probable that reselectInvokers is empty however all invoker list may contain available invokers.
        //            Use reselectCount can reduce retry times if invokers' size is huge, which may lead to long time hang up.
        if (reselectCount >= invokers.size()) {
            for (Invoker<T> invoker : invokers) {
                // check if available
                if (availableCheck && !invoker.isAvailable()) {
                    // add to invalidate invoker
                    invalidateInvoker(invoker);
                    continue;
                }

                if (selected == null || !selected.contains(invoker)) {
                    reselectInvokers.add(invoker);
                }
            }
        } else {
            for (int i = 0; i < reselectCount; i++) {     // 将不在 selected 集合中的 Invoker 过滤出来进行负载均衡
                // select one randomly
                Invoker<T> invoker = invokers.get(ThreadLocalRandom.current().nextInt(invokers.size()));
                // check if available
                if (availableCheck && !invoker.isAvailable()) {
                    // add to invalidate invoker
                    invalidateInvoker(invoker);
                    continue;
                }
                // de-duplication
                if (selected == null || !selected.contains(invoker) || !reselectInvokers.contains(invoker)) {
                    reselectInvokers.add(invoker);
                }
            }
        }

        // 2. Use loadBalance to select one (all the reselectInvokers are available)
        if (!reselectInvokers.isEmpty()) {         // reselectInvokers 不为空时，才需要通过负载均衡组件进行选择
            return loadbalance.select(reselectInvokers, getUrl(), invocation);
        }

        // 3. reselectInvokers is empty. Unable to find at least one available invoker.
        //    Re-check all the selected invokers. If some in the selected list are available, add to reselectInvokers.
        if (selected != null) {      // 只能对 selected 集合中可用的 Invoker 再次进行负载均衡
            for (Invoker<T> invoker : selected) {
                if ((invoker.isAvailable()) // available first
                    && !reselectInvokers.contains(invoker)) {
                    reselectInvokers.add(invoker);
                }
            }
        }

        // 4. If reselectInvokers is not empty after re-check.
        //    Pick an available invoker using loadBalance policy
        if (!reselectInvokers.isEmpty()) {
            return loadbalance.select(reselectInvokers, getUrl(), invocation);
        }

        // 5. No invoker match, return null.
        return null;
    }

    private void checkShouldInvalidateInvoker(Invoker<T> invoker) {
        if (availableCheck && !invoker.isAvailable()) {
            invalidateInvoker(invoker);
        }
    }

    private void invalidateInvoker(Invoker<T> invoker) {
        if (enableConnectivityValidation) {
            if (getDirectory() != null) {
                getDirectory().addInvalidateInvoker(invoker);
            }
        }
    }

    @Override
    public Result invoke(final Invocation invocation) throws RpcException {
        checkWhetherDestroyed();          // 检测当前Invoker是否已销毁

        // binding attachments into invocation.
        // 将RpcContext中的attachment添加到Invocation中
//        Map<String, Object> contextAttachments = RpcContext.getClientAttachment().getObjectAttachments();
//        if (contextAttachments != null && contextAttachments.size() != 0) {
//            ((RpcInvocation) invocation).addObjectAttachmentsIfAbsent(contextAttachments);
//        }

        InvocationProfilerUtils.enterDetailProfiler(invocation, () -> "Router route.");
        // 通过Directory获取Invoker对象列表，通过对RegistryDirectory的介绍我们知道，其中已经调用了Router进行过滤
        List<Invoker<T>> invokers = list(invocation);
        InvocationProfilerUtils.releaseDetailProfiler(invocation);

        LoadBalance loadbalance = initLoadBalance(invokers, invocation);              // 通过SPI加载LoadBalance
        RpcUtils.attachInvocationIdIfAsync(getUrl(), invocation);

        InvocationProfilerUtils.enterDetailProfiler(invocation, () -> "Cluster " + this.getClass().getName() + " invoke.");
        try {
            return doInvoke(invocation, invokers, loadbalance);         // 调用 doInvoke() 方法，该方法是个抽象方法
        } finally {
            InvocationProfilerUtils.releaseDetailProfiler(invocation);
        }
    }

    protected void checkWhetherDestroyed() {
        if (destroyed.get()) {
            throw new RpcException("Rpc cluster invoker for " + getInterface() + " on consumer " + NetUtils.getLocalHost()
                + " use dubbo version " + Version.getVersion()
                + " is now destroyed! Can not invoke any more.");
        }
    }

    @Override
    public String toString() {
        return getInterface() + " -> " + getUrl().toString();
    }

    protected void checkInvokers(List<Invoker<T>> invokers, Invocation invocation) {
        if (CollectionUtils.isEmpty(invokers)) {
            throw new RpcException(RpcException.NO_INVOKER_AVAILABLE_AFTER_FILTER, "Failed to invoke the method "
                + invocation.getMethodName() + " in the service " + getInterface().getName()
                + ". No provider available for the service " + getDirectory().getConsumerUrl().getServiceKey()
                + " from registry " + getDirectory().getUrl().getAddress()
                + " on the consumer " + NetUtils.getLocalHost()
                + " using the dubbo version " + Version.getVersion()
                + ". Please check if the providers have been started and registered.");
        }
    }

    protected Result invokeWithContext(Invoker<T> invoker, Invocation invocation) {
        setContext(invoker);
        Result result;
        try {
            if (ProfilerSwitch.isEnableSimpleProfiler()) {
                InvocationProfilerUtils.enterProfiler(invocation, "Invoker invoke. Target Address: " + invoker.getUrl().getAddress());
            }
            result = invoker.invoke(invocation);
        } finally {
            clearContext(invoker);
            InvocationProfilerUtils.releaseSimpleProfiler(invocation);
        }
        return result;
    }

    /**
     * When using a thread pool to fork a child thread, ThreadLocal cannot be passed.
     * In this scenario, please use the invokeWithContextAsync method.
     *
     * @return
     */
    protected Result invokeWithContextAsync(Invoker<T> invoker, Invocation invocation, URL consumerUrl) {
        setContext(invoker, consumerUrl);
        Result result;
        try {
            result = invoker.invoke(invocation);         // 请求对应的Provider节点
        } finally {
            clearContext(invoker);
        }
        return result;
    }

    protected abstract Result doInvoke(Invocation invocation, List<Invoker<T>> invokers,
                                       LoadBalance loadbalance) throws RpcException;

    protected List<Invoker<T>> list(Invocation invocation) throws RpcException {
        return getDirectory().list(invocation);     // 调用Directory.list()方法
    }

    /**
     * Init LoadBalance.
     * <p>
     * if invokers is not empty, init from the first invoke's url and invocation
     * if invokes is empty, init a default LoadBalance(RandomLoadBalance)
     * </p>
     *
     * @param invokers   invokers
     * @param invocation invocation
     * @return LoadBalance instance. if not need init, return null.
     */
    protected LoadBalance initLoadBalance(List<Invoker<T>> invokers, Invocation invocation) {
        ApplicationModel applicationModel = ScopeModelUtil.getApplicationModel(invocation.getModuleModel());
        if (CollectionUtils.isNotEmpty(invokers)) {
            return applicationModel.getExtensionLoader(LoadBalance.class).getExtension(
                invokers.get(0).getUrl().getMethodParameter(
                    RpcUtils.getMethodName(invocation), LOADBALANCE_KEY, DEFAULT_LOADBALANCE
                )
            );
        } else {
            return applicationModel.getExtensionLoader(LoadBalance.class).getExtension(DEFAULT_LOADBALANCE);
        }
    }


    private void setContext(Invoker<T> invoker) {
        setContext(invoker, null);
    }

    private void setContext(Invoker<T> invoker, URL consumerUrl) {
        RpcContext context = RpcContext.getServiceContext();
        context.setInvoker(invoker)
            .setConsumerUrl(null != consumerUrl ? consumerUrl : RpcContext.getServiceContext().getConsumerUrl());
    }

    private void clearContext(Invoker<T> invoker) {
        // do nothing
        RpcContext context = RpcContext.getServiceContext();
        context.setInvoker(null);
    }
}
