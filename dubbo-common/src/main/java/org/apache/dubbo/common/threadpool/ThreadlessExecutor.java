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
package org.apache.dubbo.common.threadpool;

import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * 特殊类型的线程池，对于同步请求，会使用 ThreadlessExecutor，ThreadlessExecutor 内部不管理任何线程。
 *
 * 因为在 Dubbo 2.7.5 版本之前，在 WrappedChannelHandler 中会为每个连接启动一个线程池。
 *
 * The most important difference between this Executor and other normal Executor is that this one doesn't manage any thread.
 * <p>
 * Tasks submitted to this executor through {@link #execute(Runnable)} will not get scheduled to a specific thread, though normal executors always do the schedule.
 * Those tasks are stored in a blocking queue and will only be executed when a thread calls {@link #waitAndDrain()}, the thread executing the task
 * is exactly the same as the one calling waitAndDrain.
 */
public class ThreadlessExecutor extends AbstractExecutorService {
    private static final Logger logger = LoggerFactory.getLogger(ThreadlessExecutor.class.getName());

    private final BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>();      // 阻塞队列，用来在 IO 线程和业务线程之间传递任务。

    private CompletableFuture<?> waitingFuture;     // 指向请求对应的 DefaultFuture 对象

    private boolean finished = false;

    // 当后续再次调用 execute() 方法提交任务时，会根据 waiting 字段决定任务是放入 queue 队列等待业务线程执行，还是直接由 sharedExecutor 线程池执行。
    private volatile boolean waiting = true;

    private final Object lock = new Object();

    public CompletableFuture<?> getWaitingFuture() {
        return waitingFuture;
    }

    public void setWaitingFuture(CompletableFuture<?> waitingFuture) {
        this.waitingFuture = waitingFuture;
    }

    private boolean isFinished() {
        return finished;
    }

    private void setFinished(boolean finished) {
        this.finished = finished;
    }

    public boolean isWaiting() {
        return waiting;
    }

    private void setWaiting(boolean waiting) {
        this.waiting = waiting;
    }

    /**
     * 执行任务的与调用 waitAndDrain() 方法的是同一个线程。
     *
     * Waits until there is a task, executes the task and all queued tasks (if there're any). The task is either a normal
     * response or a timeout response.
     */
    public void waitAndDrain() throws InterruptedException {
        /**
         * Usually, {@link #waitAndDrain()} will only get called once. It blocks for the response for the first time,
         * once the response (the task) reached and being executed waitAndDrain will return, the whole request process
         * then finishes. Subsequent calls on {@link #waitAndDrain()} (if there're any) should return immediately.
         *
         * There's no need to worry that {@link #finished} is not thread-safe. Checking and updating of
         * 'finished' only appear in waitAndDrain, since waitAndDrain is binding to one RPC call (one thread), the call
         * of it is totally sequential.
         */
        if (isFinished()) {     // 检测 finished 字段值，然后获取阻塞队列中的全部任务并执行
            return;
        }

        Runnable runnable;
        try {
            runnable = queue.take();
        } catch (InterruptedException e) {
            setWaiting(false);
            throw e;
        }

        synchronized (lock) {
            setWaiting(false);
            runnable.run();
        }

        runnable = queue.poll();
        while (runnable != null) {
            runnable.run();
            runnable = queue.poll();
        }
        // mark the status of ThreadlessExecutor as finished.
        // 执行完成之后会修改finished和 waiting 字段，标识当前 ThreadlessExecutor 已使用完毕，无业务线程等待
        setFinished(true);
    }

    /**
     * 将任务提交给这个线程池，但是这些提交的任务不会被调度到任何线程执行，而是存储在阻塞队列中，
     * 只有当其他线程调用 ThreadlessExecutor.waitAndDrain() 方法时才会真正执行。
     *
     * If the calling thread is still waiting for a callback task, add the task into the blocking queue to wait for schedule.
     * Otherwise, submit to shared callback executor directly.
     *
     * @param runnable
     */
    @Override
    public void execute(Runnable runnable) {
        runnable = new RunnableWrapper(runnable);
        synchronized (lock) {
            if (!isWaiting()) {  // 判断业务线程是否还在等待响应结果
                runnable.run();  // 不等待，则直接处理任务
                return;
            }
            queue.add(runnable);        // 业务线程还在等待，则将任务写入队列，然后由业务线程自己执行
        }
    }

    /**
     * tells the thread blocking on {@link #waitAndDrain()} to return, despite of the current status, to avoid endless waiting.
     */
    public void notifyReturn(Throwable t) {
        // an empty runnable task.
        execute(() -> waitingFuture.completeExceptionally(t));
    }

    /**
     * The following methods are still not supported
     */

    @Override
    public void shutdown() {
        shutdownNow();
    }

    @Override
    public List<Runnable> shutdownNow() {
        notifyReturn(new IllegalStateException("Consumer is shutting down and this call is going to be stopped without " +
                "receiving any result, usually this is called by a slow provider instance or bad service implementation."));
        return Collections.emptyList();
    }

    @Override
    public boolean isShutdown() {
        return false;
    }

    @Override
    public boolean isTerminated() {
        return false;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return false;
    }

    private static class RunnableWrapper implements Runnable {
        private Runnable runnable;

        public RunnableWrapper(Runnable runnable) {
            this.runnable = runnable;
        }

        @Override
        public void run() {
            try {
                runnable.run();
            } catch (Throwable t) {
                logger.info(t);
            }
        }
    }
}
