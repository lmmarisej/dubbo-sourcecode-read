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
package org.apache.dubbo.remoting.exchange.support;

import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.resource.GlobalResourceInitializer;
import org.apache.dubbo.common.threadpool.ThreadlessExecutor;
import org.apache.dubbo.common.timer.HashedWheelTimer;
import org.apache.dubbo.common.timer.Timeout;
import org.apache.dubbo.common.timer.Timer;
import org.apache.dubbo.common.timer.TimerTask;
import org.apache.dubbo.common.utils.NamedThreadFactory;
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.TimeoutException;
import org.apache.dubbo.remoting.exchange.Request;
import org.apache.dubbo.remoting.exchange.Response;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_TIMEOUT;
import static org.apache.dubbo.common.constants.CommonConstants.TIMEOUT_KEY;

/**
 * 表示此次请求-响应是否完成，也就是说，要收到响应为 Future 才算完成。
 *
 * 无论是 SYNC 模式、ASYNC 模式还是 FUTURE 模式，都是围绕 DefaultFuture 展开的。
 */
public class DefaultFuture extends CompletableFuture<Object> {

    private static final Logger logger = LoggerFactory.getLogger(DefaultFuture.class);

    // 管理请求与 Channel 之间的关联关系，其中 Key 为请求 ID，Value 为发送请求的 Channel。
    private static final Map<Long, Channel> CHANNELS = new ConcurrentHashMap<>();

    // 管理请求与 DefaultFuture 之间的关联关系，其中 Key 为请求 ID，Value 为请求对应的 Future。
    private static final Map<Long, DefaultFuture> FUTURES = new ConcurrentHashMap<>();

    private static final GlobalResourceInitializer<Timer> TIME_OUT_TIMER = new GlobalResourceInitializer<>(() -> new HashedWheelTimer(
        new NamedThreadFactory("dubbo-future-timeout", true), 30, TimeUnit.MILLISECONDS),
        DefaultFuture::destroy);

    // invoke id.
    private final Long id;          // 请求的 ID。
    private final Channel channel;  // 发送请求的 Channel。
    private final Request request;  // 对应请求。
    private final int timeout;      // 整个请求-响应交互完成的超时时间。
    private final long start = System.currentTimeMillis();     // 该 DefaultFuture 的创建时间。
    private volatile long sent;     // 请求发送的时间。
    private Timeout timeoutCheckTask;   // 每个 DefaultFuture 实例关联一个定时任务，该定时任务到期时，表示对端响应超时。

    private ExecutorService executor;   // 请求关联的线程池。

    public ExecutorService getExecutor() {
        return executor;
    }

    public void setExecutor(ExecutorService executor) {
        this.executor = executor;
    }

    private DefaultFuture(Channel channel, Request request, int timeout) {
        this.channel = channel;
        this.request = request;
        this.id = request.getId();
        this.timeout = timeout > 0 ? timeout : channel.getUrl().getPositiveParameter(TIMEOUT_KEY, DEFAULT_TIMEOUT);
        // put into waiting map.
        FUTURES.put(id, this);
        CHANNELS.put(id, channel);
    }

    /**
     * 创建定时任务，提交到时间轮中等待执行。用于处理 future 对应的请求超时。
     */
    private static void timeoutCheck(DefaultFuture future) {
        TimeoutCheckTask task = new TimeoutCheckTask(future.getId());
        // 创建定时任务，与当前 DefaultFuture 实例进行关联
        future.timeoutCheckTask = TIME_OUT_TIMER.get().newTimeout(task, future.getTimeout(), TimeUnit.MILLISECONDS);
    }

    public static void destroy() {
        TIME_OUT_TIMER.remove(Timer::stop);
        FUTURES.clear();
        CHANNELS.clear();
    }

    /**
     * 创建 DefaultFuture 对象时，需要初始化实例字段，并创建请求相应的超时定时任务。
     *
     * init a DefaultFuture
     * 1.init a DefaultFuture
     * 2.timeout check
     *
     * @param channel channel
     * @param request the request
     * @param timeout timeout
     * @return a new DefaultFuture
     */
    public static DefaultFuture newFuture(Channel channel, Request request, int timeout, ExecutorService executor) {
        final DefaultFuture future = new DefaultFuture(channel, request, timeout);
        future.setExecutor(executor);
        // ThreadlessExecutor needs to hold the waiting future in case of circuit return.
        if (executor instanceof ThreadlessExecutor) {
            ((ThreadlessExecutor) executor).setWaitingFuture(future);   // ThreadlessExecutor 可以关联一个 waitingFuture
        }
        // timeout check
        timeoutCheck(future);        // 创建一个定时任务，用处理响应超时的情况
        return future;
    }

    public static DefaultFuture getFuture(long id) {
        return FUTURES.get(id);
    }

    public static boolean hasFuture(Channel channel) {
        return CHANNELS.containsValue(channel);
    }

    public static void sent(Channel channel, Request request) {
        DefaultFuture future = FUTURES.get(request.getId());
        if (future != null) {
            future.doSent();
        }
    }

    /**
     * 创建并传递一个 Response，该 Response 的状态码为 CHANNEL_INACTIVE
     *
     * close a channel when a channel is inactive
     * directly return the unfinished requests.
     *
     * @param channel channel to close
     */
    public static void closeChannel(Channel channel) {
        for (Map.Entry<Long, Channel> entry : CHANNELS.entrySet()) {
            if (channel.equals(entry.getValue())) {
                DefaultFuture future = getFuture(entry.getKey());
                if (future != null && !future.isDone()) {
                    Response disconnectResponse = new Response(future.getId());
                    disconnectResponse.setStatus(Response.CHANNEL_INACTIVE);
                    disconnectResponse.setErrorMessage("Channel " +
                            channel +
                            " is inactive. Directly return the unFinished request : " +
                            (logger.isDebugEnabled() ? future.getRequest() : future.getRequest().copyWithoutData()));
                    DefaultFuture.received(channel, disconnectResponse);
                }
            }
        }
    }

    public static void received(Channel channel, Response response) {
        received(channel, response, false);
    }

    /**
     * 找到响应关联的 DefaultFuture 对象，调用 doReceived() 方法，将 DefaultFuture 设置为完成状态。
     */
    public static void received(Channel channel, Response response, boolean timeout) {
        try {
            DefaultFuture future = FUTURES.remove(response.getId());     // 清理请求 ID 与 DefaultFuture 之间的映射关系
            if (future != null) {
                Timeout t = future.timeoutCheckTask;        // 超时检查
                if (!timeout) {         // 未超时，取消定时任务
                    // decrease Time
                    t.cancel();
                }
                future.doReceived(response);
            } else {
                // 服务端发送了一个未知的响应报文
                logger.warn("The timeout response finally returned at "
                        + (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date()))
                        + ", response status is " + response.getStatus()
                        + (channel == null ? "" : ", channel: " + channel.getLocalAddress()
                        + " -> " + channel.getRemoteAddress()) + ", please check provider side for detailed result.");
            }
        } finally {
            CHANNELS.remove(response.getId());      // 取消请求关联的对 Channel 的引用
        }
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        Response errorResult = new Response(id);
        errorResult.setStatus(Response.CLIENT_ERROR);
        errorResult.setErrorMessage("request future has been canceled.");
        this.doReceived(errorResult);
        FUTURES.remove(id);
        CHANNELS.remove(id);
        timeoutCheckTask.cancel();
        return true;
    }

    public void cancel() {
        this.cancel(true);
    }

    private void doReceived(Response res) {
        if (res == null) {
            throw new IllegalStateException("response cannot be null");
        }
        if (res.getStatus() == Response.OK) {        // 正常响应
            this.complete(res.getResult());
        } else if (res.getStatus() == Response.CLIENT_TIMEOUT || res.getStatus() == Response.SERVER_TIMEOUT) {  // 超时
            this.completeExceptionally(new TimeoutException(res.getStatus() == Response.SERVER_TIMEOUT, channel, res.getErrorMessage()));
        } else {        // 其他异常
            this.completeExceptionally(new RemotingException(channel, res.getErrorMessage()));
        }

        // the result is returning, but the caller thread may still waiting
        // to avoid endless waiting for whatever reason, notify caller thread to return.
        if (executor != null && executor instanceof ThreadlessExecutor) {       // 兜底处理，防止业务线程一直阻塞在 ThreadlessExecutor 上
            ThreadlessExecutor threadlessExecutor = (ThreadlessExecutor) executor;
            if (threadlessExecutor.isWaiting()) {
                // 向 ThreadlessExecutor 提交一个异常类型任务，避免业务线程阻塞
                threadlessExecutor.notifyReturn(new IllegalStateException("The result has returned, but the biz thread is still waiting" +
                        " which is not an expected state, interrupt the thread manually by returning an exception."));
            }
        }
    }

    private long getId() {
        return id;
    }

    private Channel getChannel() {
        return channel;
    }

    private boolean isSent() {
        return sent > 0;
    }

    public Request getRequest() {
        return request;
    }

    private int getTimeout() {
        return timeout;
    }

    /**
     * 记录请求发送时间。
     */
    private void doSent() {
        sent = System.currentTimeMillis();
    }

    private String getTimeoutMessage(boolean scan) {
        long nowTimestamp = System.currentTimeMillis();
        return (sent > 0 ? "Waiting server-side response timeout" : "Sending request timeout in client-side")
                + (scan ? " by scan timer" : "") + ". start time: "
                + (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date(start))) + ", end time: "
                + (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date(nowTimestamp))) + ","
                + (sent > 0 ? " client elapsed: " + (sent - start)
                + " ms, server elapsed: " + (nowTimestamp - sent)
                : " elapsed: " + (nowTimestamp - start)) + " ms, timeout: "
                + timeout + " ms, request: " + (logger.isDebugEnabled() ? request : request.copyWithoutData()) + ", channel: " + channel.getLocalAddress()
                + " -> " + channel.getRemoteAddress();
    }


    private static class TimeoutCheckTask implements TimerTask {

        private final Long requestID;

        TimeoutCheckTask(Long requestID) {
            this.requestID = requestID;
        }

        @Override
        public void run(Timeout timeout) {
            DefaultFuture future = DefaultFuture.getFuture(requestID);
            if (future == null || future.isDone()) {
                return;
            }

            if (future.getExecutor() != null) {
                future.getExecutor().execute(() -> notifyTimeout(future));
            } else {
                notifyTimeout(future);
            }
        }

        /**
         * 发出的请求在限定时间内未响应，本方法触发。
         */
        private void notifyTimeout(DefaultFuture future) {
            // create exception response.
            // 没有收到对端的响应，这里会创建一个 Response，表示超时的响应
            Response timeoutResponse = new Response(future.getId());
            // set timeout status.
            timeoutResponse.setStatus(future.isSent() ? Response.SERVER_TIMEOUT : Response.CLIENT_TIMEOUT);
            timeoutResponse.setErrorMessage(future.getTimeoutMessage(true));
            // handle response.
            // 将关联的 DefaultFuture 标记为超时异常完成
            DefaultFuture.received(future.getChannel(), timeoutResponse, true);
        }
    }
}
