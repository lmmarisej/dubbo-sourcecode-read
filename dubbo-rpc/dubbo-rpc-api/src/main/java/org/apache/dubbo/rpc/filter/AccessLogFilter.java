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
package org.apache.dubbo.rpc.filter;

import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.threadpool.manager.FrameworkExecutorRepository;
import org.apache.dubbo.common.utils.ConcurrentHashSet;
import org.apache.dubbo.common.utils.ConfigUtils;
import org.apache.dubbo.rpc.*;
import org.apache.dubbo.rpc.support.AccessLogData;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.dubbo.common.constants.CommonConstants.PROVIDER;
import static org.apache.dubbo.rpc.Constants.ACCESS_LOG_KEY;

/**
 * 主要用于记录日志，它的主要功能是将 Provider 或者 Consumer 的日志信息写入文件中。
 * <p>
 * Record access log for the service.
 * <p>
 * Logger key is <code><b>dubbo.accesslog</b></code>.
 * In order to configure access log appear in the specified appender only, additivity need to be configured in log4j's
 * config file, for example:
 * <code>
 * <pre>
 * &lt;logger name="<b>dubbo.accesslog</b>" <font color="red">additivity="false"</font>&gt;
 *    &lt;level value="info" /&gt;
 *    &lt;appender-ref ref="foo" /&gt;
 * &lt;/logger&gt;
 * </pre></code>
 */
@Activate(group = PROVIDER, value = ACCESS_LOG_KEY)
public class AccessLogFilter implements Filter {

    private static final Logger logger = LoggerFactory.getLogger(AccessLogFilter.class);

    private static final String LOG_KEY = "dubbo.accesslog";

    private static final int LOG_MAX_BUFFER = 5000;

    private static final long LOG_OUTPUT_INTERVAL = 5000;

    private static final String FILE_DATE_FORMAT = "yyyyMMdd";

    // It's safe to declare it as singleton since it runs on single thread only
    private final DateFormat fileNameFormatter = new SimpleDateFormat(FILE_DATE_FORMAT);

    private final Map<String, Set<AccessLogData>> logEntries = new ConcurrentHashMap<>();

    private AtomicBoolean scheduled = new AtomicBoolean();

    /**
     * Default constructor initialize demon thread for writing into access log file with names with access log key
     * defined in url <b>accesslog</b>
     */
    public AccessLogFilter() {
    }

    /**
     * 先将日志消息放入内存日志集合中缓存，当缓存大小超过一定阈值之后，会触发日志的写入。若长时间未触发日志文件写入，则由定时任务定时写入。
     *
     * @param invoker service
     * @param inv     Invocation service method.
     * @return Result from service method.
     * @throws RpcException
     */
    @Override
    public Result invoke(Invoker<?> invoker, Invocation inv) throws RpcException {
        if (scheduled.compareAndSet(false, true)) {
            inv.getModuleModel().getApplicationModel().getFrameworkModel().getBeanFactory()
                .getBean(FrameworkExecutorRepository.class).getSharedScheduledExecutor()    // 启动一个线程池
                // 启动一个定时任务，定期执行 writeLogSetToFile() 方法，完成日志写入
                .scheduleWithFixedDelay(this::writeLogToFile, LOG_OUTPUT_INTERVAL, LOG_OUTPUT_INTERVAL, TimeUnit.MILLISECONDS);
        }
        try {
            String accessLogKey = invoker.getUrl().getParameter(ACCESS_LOG_KEY);         // 获取ACCESS_LOG_KEY
            if (ConfigUtils.isNotEmpty(accessLogKey)) {
                // 构造 AccessLogData 对象，其中记录了日志信息，例如，调用的服务名称、方法名称、version等
                AccessLogData logData = AccessLogData.newLogData();
                logData.buildAccessLogData(invoker, inv);
                log(accessLogKey, logData);
            }
        } catch (Throwable t) {
            logger.warn("Exception in AccessLogFilter of service(" + invoker + " -> " + inv + ")", t);
        }
        return invoker.invoke(inv);         // 调用下一个 Invoker
    }

    /**
     * 按照 ACCESS_LOG_KEY 的值，找到对应的 AccessLogData 集合，然后完成缓存写入；如果缓存大小超过阈值，则触发文件写入。
     */
    private void log(String accessLog, AccessLogData accessLogData) {
        // 根据 ACCESS_LOG_KEY 获取对应的缓存集合
        Set<AccessLogData> logSet = logEntries.computeIfAbsent(accessLog, k -> new ConcurrentHashSet<>());

        if (logSet.size() < LOG_MAX_BUFFER) {       // 缓存大小未超过阈值
            logSet.add(accessLogData);
        } else {         // 缓存大小超过阈值，触发缓存数据写入文件
            logger.warn("AccessLog buffer is full. Do a force writing to file to clear buffer.");
            //just write current logSet to file.
            writeLogSetToFile(accessLog, logSet);
            //after force writing, add accessLogData to current logSet
            logSet.add(accessLogData);            // 完成文件写入之后，再次写入缓存
        }
    }

    /**
     * 按照 ACCESS_LOG_KEY 的值将日志信息写入不同的日志文件中。
     * <p>
     * 值不为 true 或 default，则 ACCESS_LOG_KEY 配置值会被当作 access log 文件的名称，AccessLogFilter 会创建相应的目录和文件，并完成日志的输出。
     */
    private void writeLogSetToFile(String accessLog, Set<AccessLogData> logSet) {
        try {
            if (ConfigUtils.isDefault(accessLog)) {
                processWithServiceLogger(logSet);            // ACCESS_LOG_KEY 配置值为 true 或是 default
            } else {         // ACCESS_LOG_KEY 配置既不是 true 也不是 default
                File file = new File(accessLog);
                createIfLogDirAbsent(file);     // 创建目录
                if (logger.isDebugEnabled()) {
                    logger.debug("Append log to " + accessLog);
                }
                renameFile(file);       // 创建日志文件，这里会以日期为后缀，滚动创建
                processWithAccessKeyLogger(logSet, file);      // 遍历 logSet 集合，将日志逐条写入文件
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private void writeLogToFile() {
        if (!logEntries.isEmpty()) {
            for (Map.Entry<String, Set<AccessLogData>> entry : logEntries.entrySet()) {
                String accessLog = entry.getKey();
                Set<AccessLogData> logSet = entry.getValue();
                writeLogSetToFile(accessLog, logSet);
            }
        }
    }

    private void processWithAccessKeyLogger(Set<AccessLogData> logSet, File file) throws IOException {
        // 创建 FileWriter，写入指定的日志文件
        try (FileWriter writer = new FileWriter(file, true)) {
            for (Iterator<AccessLogData> iterator = logSet.iterator();
                 iterator.hasNext();
                 iterator.remove()) {
                writer.write(iterator.next().getLogMessage());
                writer.write(System.getProperty("line.separator"));
            }
            writer.flush();
        }
    }

    private AccessLogData buildAccessLogData(Invoker<?> invoker, Invocation inv) {
        AccessLogData logData = AccessLogData.newLogData();
        logData.setServiceName(invoker.getInterface().getName());
        logData.setMethodName(inv.getMethodName());
        logData.setVersion(invoker.getUrl().getVersion());
        logData.setGroup(invoker.getUrl().getGroup());
        logData.setInvocationTime(new Date());
        logData.setTypes(inv.getParameterTypes());
        logData.setArguments(inv.getArguments());
        return logData;
    }

    private void processWithServiceLogger(Set<AccessLogData> logSet) {
        for (Iterator<AccessLogData> iterator = logSet.iterator();
             iterator.hasNext();
            // 遍历 logSet 集合
             iterator.remove()) {
            AccessLogData logData = iterator.next();
            // 通过 LoggerFactory 获取 Logger 对象，并写入日志
            LoggerFactory.getLogger(LOG_KEY + "." + logData.getServiceName()).info(logData.getLogMessage());
        }
    }

    private void createIfLogDirAbsent(File file) {
        File dir = file.getParentFile();
        if (null != dir && !dir.exists()) {
            dir.mkdirs();
        }
    }

    private void renameFile(File file) {
        if (file.exists()) {
            String now = fileNameFormatter.format(new Date());
            String last = fileNameFormatter.format(new Date(file.lastModified()));
            if (!now.equals(last)) {
                File archive = new File(file.getAbsolutePath() + "." + last);
                file.renameTo(archive);
            }
        }
    }
}
