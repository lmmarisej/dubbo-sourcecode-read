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
package org.apache.dubbo.remoting.exchange.support.header;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.resource.GlobalResourceInitializer;
import org.apache.dubbo.common.timer.HashedWheelTimer;
import org.apache.dubbo.common.timer.Timeout;
import org.apache.dubbo.common.utils.Assert;
import org.apache.dubbo.common.utils.NamedThreadFactory;
import org.apache.dubbo.remoting.ChannelHandler;
import org.apache.dubbo.remoting.Client;
import org.apache.dubbo.remoting.Constants;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.exchange.ExchangeChannel;
import org.apache.dubbo.remoting.exchange.ExchangeClient;
import org.apache.dubbo.remoting.exchange.ExchangeHandler;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.dubbo.remoting.Constants.*;
import static org.apache.dubbo.remoting.utils.UrlUtils.getHeartbeat;
import static org.apache.dubbo.remoting.utils.UrlUtils.getIdleTimeout;

/**
 * 是 Client 装饰器，主要为其装饰的 Client 添加两个功能：
 * 1.维持与 Server 的长连状态，这是通过定时发送心跳消息实现的；
 * 2.在因故障掉线之后，进行重连，这是通过定时检查连接状态实现的。
 */
public class HeaderExchangeClient implements ExchangeClient {

    private final Client client;                // HeaderExchangeClient 装饰 Client
    private final ExchangeChannel channel;      // 与服务端建立的连接，HeaderExchangeClient 装饰 Channel

    public static GlobalResourceInitializer<HashedWheelTimer> IDLE_CHECK_TIMER = new GlobalResourceInitializer<>(() ->
        new HashedWheelTimer(new NamedThreadFactory("dubbo-client-idleCheck", true), 1,
            TimeUnit.SECONDS, TICKS_PER_WHEEL),
        HashedWheelTimer::stop);

    private Timeout reconnectTimer;
    private Timeout heartBeatTimer;

    /**
     * @param client     封装 Transport 层的 Client 对象。
     * @param startTimer 控制是否开启心跳定时任务和重连定时任务。
     */
    public HeaderExchangeClient(Client client, boolean startTimer) {
        Assert.notNull(client, "Client can't be null");
        this.client = client;
        this.channel = new HeaderExchangeChannel(client);

        if (startTimer) {
            URL url = client.getUrl();
            startReconnectTask(url);
            startHeartBeatTask(url);
        }
    }

    @Override
    public CompletableFuture<Object> request(Object request) throws RemotingException {
        return channel.request(request);
    }

    @Override
    public URL getUrl() {
        return channel.getUrl();
    }

    @Override
    public InetSocketAddress getRemoteAddress() {
        return channel.getRemoteAddress();
    }

    @Override
    public CompletableFuture<Object> request(Object request, int timeout) throws RemotingException {
        return channel.request(request, timeout);
    }

    @Override
    public CompletableFuture<Object> request(Object request, ExecutorService executor) throws RemotingException {
        return channel.request(request, executor);
    }

    @Override
    public CompletableFuture<Object> request(Object request, int timeout, ExecutorService executor) throws RemotingException {
        return channel.request(request, timeout, executor);
    }

    @Override
    public ChannelHandler getChannelHandler() {
        return channel.getChannelHandler();
    }

    @Override
    public boolean isConnected() {
        return channel.isConnected();
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return channel.getLocalAddress();
    }

    @Override
    public ExchangeHandler getExchangeHandler() {
        return channel.getExchangeHandler();
    }

    @Override
    public void send(Object message) throws RemotingException {
        channel.send(message);
    }

    @Override
    public void send(Object message, boolean sent) throws RemotingException {
        channel.send(message, sent);
    }

    @Override
    public boolean isClosed() {
        return channel.isClosed();
    }

    @Override
    public void close() {
        doClose();
        channel.close();
    }

    /**
     * 如果当前 Channel 上还有请求未收到响应，会循环等待至收到响应。
     * 如果超时未收到响应，会自己创建一个状态码将连接关闭的 Response 交给 DefaultFuture 处理，与收到 disconnected 事件相同。
     * <p>
     * 然后会关闭 Transport 层的 Channel，以 NettyChannel 为例，NettyChannel.close() 方法会先将自身的 closed 字段设置为 true，
     * 清理 CHANNEL_MAP 缓存中的记录，以及 Channel 的附加属性，最后才是关闭 io.netty.channel.Channel。
     */
    @Override
    public void close(int timeout) {
        // Mark the client into the closure process
        startClose();       // 会将自身的 closed 字段设置为 true，这样就不会继续发送请求。
        doClose();
        channel.close(timeout);
    }

    @Override
    public void startClose() {
        channel.startClose();
    }

    @Override
    public void reset(URL url) {
        client.reset(url);
        // FIXME, should cancel and restart timer tasks if parameters in the new URL are different?
    }

    @Override
    @Deprecated
    public void reset(org.apache.dubbo.common.Parameters parameters) {
        reset(getUrl().addParameters(parameters.getParameters()));
    }

    @Override
    public void reconnect() throws RemotingException {
        client.reconnect();
    }

    @Override
    public Object getAttribute(String key) {
        return channel.getAttribute(key);
    }

    @Override
    public void setAttribute(String key, Object value) {
        channel.setAttribute(key, value);
    }

    @Override
    public void removeAttribute(String key) {
        channel.removeAttribute(key);
    }

    @Override
    public boolean hasAttribute(String key) {
        return channel.hasAttribute(key);
    }

    /**
     * 启动心跳定时任务。
     */
    private void startHeartBeatTask(URL url) {
        if (!client.canHandleIdle()) {              // Client 的具体实现决定是否启动该心跳任务
            AbstractTimerTask.ChannelProvider cp = () -> Collections.singletonList(HeaderExchangeClient.this);
            int heartbeat = getHeartbeat(url);      // 计算心跳间隔，最小间隔不能低于 1s
            long heartbeatTick = calculateLeastDuration(heartbeat);
            // 创建心跳任务
            HeartbeatTimerTask heartBeatTimerTask = new HeartbeatTimerTask(cp, heartbeatTick, heartbeat);
            // 提交到IDLE_CHECK_TIMER这个时间轮中等待执行
            heartBeatTimer = IDLE_CHECK_TIMER.get().newTimeout(heartBeatTimerTask, heartbeatTick, TimeUnit.MILLISECONDS);
        }
    }

    private void startReconnectTask(URL url) {
        if (shouldReconnect(url)) {     // 会根据 URL 中的参数决定是否启动任务。
            AbstractTimerTask.ChannelProvider cp = () -> Collections.singletonList(HeaderExchangeClient.this);
            int idleTimeout = getIdleTimeout(url);
            long heartbeatTimeoutTick = calculateLeastDuration(idleTimeout);
            ReconnectTimerTask reconnectTimerTask = new ReconnectTimerTask(cp, heartbeatTimeoutTick, idleTimeout);
            // 重连定时任务最终也是提交到 IDLE_CHECK_TIMER 这个时间轮中
            reconnectTimer = IDLE_CHECK_TIMER.get().newTimeout(reconnectTimerTask, heartbeatTimeoutTick, TimeUnit.MILLISECONDS);
        }
    }

    private void doClose() {
        if (heartBeatTimer != null) {
            heartBeatTimer.cancel();
            heartBeatTimer = null;
        }
        if (reconnectTimer != null) {
            reconnectTimer.cancel();
            reconnectTimer = null;
        }
    }

    /**
     * Each interval cannot be less than 1000ms.
     */
    private long calculateLeastDuration(int time) {
        if (time / HEARTBEAT_CHECK_TICK <= 0) {
            return LEAST_HEARTBEAT_DURATION;
        } else {
            return time / HEARTBEAT_CHECK_TICK;
        }
    }

    private boolean shouldReconnect(URL url) {
        return url.getParameter(Constants.RECONNECT_KEY, true);
    }

    @Override
    public String toString() {
        return "HeaderExchangeClient [channel=" + channel + "]";
    }
}
