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
package org.apache.dubbo.metadata.report.support;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.ConfigUtils;
import org.apache.dubbo.common.utils.NamedThreadFactory;
import org.apache.dubbo.metadata.definition.model.FullServiceDefinition;
import org.apache.dubbo.metadata.definition.model.ServiceDefinition;
import org.apache.dubbo.metadata.report.MetadataReport;
import org.apache.dubbo.metadata.report.identifier.KeyTypeEnum;
import org.apache.dubbo.metadata.report.identifier.MetadataIdentifier;
import org.apache.dubbo.metadata.report.identifier.ServiceMetadataIdentifier;
import org.apache.dubbo.metadata.report.identifier.SubscriberMetadataIdentifier;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.lang.reflect.Type;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.dubbo.common.constants.CommonConstants.CONSUMER_SIDE;
import static org.apache.dubbo.common.constants.CommonConstants.CYCLE_REPORT_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.FILE_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.PROVIDER_SIDE;
import static org.apache.dubbo.common.constants.CommonConstants.REPORT_DEFINITION_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.REPORT_METADATA_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.RETRY_PERIOD_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.RETRY_TIMES_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.SYNC_REPORT_KEY;
import static org.apache.dubbo.common.utils.StringUtils.replace;
import static org.apache.dubbo.metadata.report.support.Constants.CACHE;
import static org.apache.dubbo.metadata.report.support.Constants.DEFAULT_METADATA_REPORT_CYCLE_REPORT;
import static org.apache.dubbo.metadata.report.support.Constants.DEFAULT_METADATA_REPORT_RETRY_PERIOD;
import static org.apache.dubbo.metadata.report.support.Constants.DEFAULT_METADATA_REPORT_RETRY_TIMES;
import static org.apache.dubbo.metadata.report.support.Constants.DUBBO_METADATA;
import static org.apache.dubbo.metadata.report.support.Constants.USER_HOME;

/**
 * 提供了所有 MetadataReport 的公共实现
 */
public abstract class AbstractMetadataReport implements MetadataReport {

    protected final static String DEFAULT_ROOT = "dubbo";

    private static final int ONE_DAY_IN_MILLISECONDS = 60 * 24 * 60 * 1000;
    private static final int FOUR_HOURS_IN_MILLISECONDS = 60 * 4 * 60 * 1000;
    // Log output
    protected final Logger logger = LoggerFactory.getLogger(getClass());

    // Local disk cache, where the special key value.registries records the list of metadata centers, and the others are the list of notified service providers
    final Properties properties = new Properties();
    // 该线程池除了用来同步本地内存缓存与文件缓存，还会用来完成异步上报的功能
    private final ExecutorService reportCacheExecutor = Executors.newFixedThreadPool(1, new NamedThreadFactory("DubboSaveMetadataReport", true));
    final Map<MetadataIdentifier, Object> allMetadataReports = new ConcurrentHashMap<>(4);  // 内存缓存

    private final AtomicLong lastCacheChanged = new AtomicLong();       // 记录最近一次元数据上报的版本，单调递增
    final Map<MetadataIdentifier, Object> failedReports = new ConcurrentHashMap<>(4);   // 用来暂存上报失败的元数据，后面会有定时任务进行重试
    private URL reportURL;          // 元数据中心的URL，其中包含元数据中心的地址
    boolean syncReport;             // 是否同步上报元数据
    // Local disk cache file
    File file;          // 本地磁盘缓存，用来缓存上报的元数据
    private AtomicBoolean initialized = new AtomicBoolean(false);   // 当前 MetadataReport 实例是否已经初始化
    public MetadataReportRetry metadataReportRetry;                     // 用于重试的定时任务
    private ScheduledExecutorService reportTimerScheduler;

    private final boolean reportMetadata;
    private final boolean reportDefinition;

    /**
     * 初始化本地的文件缓存，然后创建 MetadataReportRetry 重试任务，并启动一个周期性刷新的定时任务
     */
    public AbstractMetadataReport(URL reportServerURL) {
        setUrl(reportServerURL);
        // Start file save timer
        String defaultFilename = System.getProperty(USER_HOME) + DUBBO_METADATA +           // 默认的本地文件缓存
            reportServerURL.getApplication() + "-" +
            replace(reportServerURL.getAddress(), ":", "-") + CACHE;
        String filename = reportServerURL.getParameter(FILE_KEY, defaultFilename);
        File file = null;
        if (ConfigUtils.isNotEmpty(filename)) {
            file = new File(filename);
            if (!file.exists() && file.getParentFile() != null && !file.getParentFile().exists()) {
                if (!file.getParentFile().mkdirs()) {
                    throw new IllegalArgumentException("Invalid service store file " + file + ", cause: Failed to create directory " + file.getParentFile() + "!");
                }
            }
            // if this file exists, firstly delete it.
            if (!initialized.getAndSet(true) && file.exists()) {
                file.delete();
            }
        }
        this.file = file;
        loadProperties();          // 将 file 文件中的内容加载到 properties 字段中
        syncReport = reportServerURL.getParameter(SYNC_REPORT_KEY, false);     // 是否同步上报元数据
        // 创建重试任务
        metadataReportRetry = new MetadataReportRetry(reportServerURL.getParameter(RETRY_TIMES_KEY, DEFAULT_METADATA_REPORT_RETRY_TIMES),
            reportServerURL.getParameter(RETRY_PERIOD_KEY, DEFAULT_METADATA_REPORT_RETRY_PERIOD));
        // cycle report the data switch
        // 是否周期性地上报元数据
        if (reportServerURL.getParameter(CYCLE_REPORT_KEY, DEFAULT_METADATA_REPORT_CYCLE_REPORT)) {
            reportTimerScheduler = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("DubboMetadataReportTimer", true));
            // 默认每隔 1 天将本地元数据全部刷新到元数据中心
            reportTimerScheduler.scheduleAtFixedRate(this::publishAll, calculateStartTime(), ONE_DAY_IN_MILLISECONDS, TimeUnit.MILLISECONDS);
        }

        this.reportMetadata = reportServerURL.getParameter(REPORT_METADATA_KEY, false);
        this.reportDefinition = reportServerURL.getParameter(REPORT_DEFINITION_KEY, true);
    }

    public URL getUrl() {
        return reportURL;
    }

    protected void setUrl(URL url) {
        if (url == null) {
            throw new IllegalArgumentException("metadataReport url == null");
        }
        this.reportURL = url;
    }

    /**
     * 刷新本地缓存文件的全部操作。
     */
    private void doSaveProperties(long version) {
        if (version < lastCacheChanged.get()) {      // 对比当前版本号和此次 SaveProperties 任务的版本号
            return;
        }
        if (file == null) {         // 检测本地缓存文件是否存在
            return;
        }
        // Save
        try {
            File lockfile = new File(file.getAbsolutePath() + ".lock");      // 创建lock文件
            if (!lockfile.exists()) {
                lockfile.createNewFile();
            }
            try (RandomAccessFile raf = new RandomAccessFile(lockfile, "rw");
                FileChannel channel = raf.getChannel()) {
                FileLock lock = channel.tryLock();              // 对lock文件加锁
                if (lock == null) {
                    throw new IOException("Can not lock the metadataReport cache file " + file.getAbsolutePath() +
                        ", ignore and retry later, maybe multi java process use the file, please config: dubbo.metadata.file=xxx.properties");
                }
                // Save
                try {
                    if (!file.exists()) {            // 保证本地缓存文件存在
                        file.createNewFile();
                    }

                    Properties tmpProperties;
                    if (!syncReport) {
                        // When syncReport = false, properties.setProperty and properties.store are called from the same
                        // thread(reportCacheExecutor), so deep copy is not required
                        tmpProperties = properties;
                    } else {
                        // Using store method and setProperty method of the this.properties will cause lock contention
                        // under multi-threading, so deep copy a new container
                        tmpProperties = new Properties();
                        Set<Map.Entry<Object, Object>> entries = properties.entrySet();
                        for (Map.Entry<Object, Object> entry : entries) {
                            tmpProperties.setProperty((String) entry.getKey(), (String) entry.getValue());
                        }
                    }

                    // 将 properties 中的元数据保存到本地缓存文件中
                    try (FileOutputStream outputFile = new FileOutputStream(file)) {
                        tmpProperties.store(outputFile, "Dubbo metadataReport Cache");
                    }
                } finally {
                    lock.release();      // 释放lock文件上的锁
                }
            }
        } catch (Throwable e) {
            if (version < lastCacheChanged.get()) {     // 比较版本号
                return;
            } else {
                // 如果写文件失败，则重新提交 SaveProperties 任务，再次尝试
                reportCacheExecutor.execute(new SaveProperties(lastCacheChanged.incrementAndGet()));
            }
            logger.warn("Failed to save service store file, cause: " + e.getMessage(), e);
        }
    }

    void loadProperties() {
        if (file != null && file.exists()) {
            try (InputStream in = Files.newInputStream(file.toPath())) {
                properties.load(in);
                if (logger.isInfoEnabled()) {
                    logger.info("Load service store file " + file + ", data: " + properties);
                }
            } catch (Throwable e) {
                logger.warn("Failed to load service store file " + file, e);
            }
        }
    }

    private void saveProperties(MetadataIdentifier metadataIdentifier, String value, boolean add, boolean sync) {
        if (file == null) {
            return;
        }

        try {
            if (add) {      // 更新 properties 中的元数据
                properties.setProperty(metadataIdentifier.getUniqueKey(KeyTypeEnum.UNIQUE_KEY), value);
            } else {
                properties.remove(metadataIdentifier.getUniqueKey(KeyTypeEnum.UNIQUE_KEY));
            }
            long version = lastCacheChanged.incrementAndGet();         // 递增版本
            if (sync) {      // 同步更新本地缓存文件
                new SaveProperties(version).run();
            } else {        // 异步更新本地缓存文件
                reportCacheExecutor.execute(new SaveProperties(version));
            }

        } catch (Throwable t) {
            logger.warn(t.getMessage(), t);
        }
    }

    @Override
    public String toString() {
        return getUrl().toString();
    }

    private class SaveProperties implements Runnable {
        private long version;

        private SaveProperties(long version) {
            this.version = version;
        }

        @Override
        public void run() {
            doSaveProperties(version);
        }
    }

    /**
     * 根据 syncReport 字段值决定是同步上报还是异步上报：
     *      - 如果是同步上报，则在当前线程执行上报操作；
     *      - 如果是异步上报，则在 reportCacheExecutor 线程池中执行上报操作。
     */
    @Override
    public void storeProviderMetadata(MetadataIdentifier providerMetadataIdentifier, ServiceDefinition serviceDefinition) {
        if (syncReport) {
            storeProviderMetadataTask(providerMetadataIdentifier, serviceDefinition);
        } else {
            reportCacheExecutor.execute(() -> storeProviderMetadataTask(providerMetadataIdentifier, serviceDefinition));
        }
    }

    private void storeProviderMetadataTask(MetadataIdentifier providerMetadataIdentifier, ServiceDefinition serviceDefinition) {
        try {
            if (logger.isInfoEnabled()) {
                logger.info("store provider metadata. Identifier : " + providerMetadataIdentifier + "; definition: " + serviceDefinition);
            }
            allMetadataReports.put(providerMetadataIdentifier, serviceDefinition);   // 将元数据记录到 allMetadataReports 集合
            failedReports.remove(providerMetadataIdentifier);   // 如果之前上报失败，则在 failedReports 集合中有记录，这里上报成功之后会将其删除
            Gson gson = new Gson();            // 将元数据序列化成JSON字符串
            String data = gson.toJson(serviceDefinition);
            doStoreProviderMetadata(providerMetadataIdentifier, data);       // 上报序列化后的元数据
            saveProperties(providerMetadataIdentifier, data, true, !syncReport);    // 将序列化后的元数据保存到本地文件缓存中
        } catch (Exception e) {
            // retry again. If failed again, throw exception.
            // 如果上报失败，则在 failedReports 集合中进行记录，然后由 metadataReportRetry 任务中进行重试
            failedReports.put(providerMetadataIdentifier, serviceDefinition);
            metadataReportRetry.startRetryTask();
            logger.error("Failed to put provider metadata " + providerMetadataIdentifier + " in  " + serviceDefinition + ", cause: " + e.getMessage(), e);
        }
    }

    @Override
    public void storeConsumerMetadata(MetadataIdentifier consumerMetadataIdentifier, Map<String, String> serviceParameterMap) {
        if (syncReport) {
            storeConsumerMetadataTask(consumerMetadataIdentifier, serviceParameterMap);
        } else {
            reportCacheExecutor.execute(() -> storeConsumerMetadataTask(consumerMetadataIdentifier, serviceParameterMap));
        }
    }

    protected void storeConsumerMetadataTask(MetadataIdentifier consumerMetadataIdentifier, Map<String, String> serviceParameterMap) {
        try {
            if (logger.isInfoEnabled()) {
                logger.info("store consumer metadata. Identifier : " + consumerMetadataIdentifier + "; definition: " + serviceParameterMap);
            }
            allMetadataReports.put(consumerMetadataIdentifier, serviceParameterMap);
            failedReports.remove(consumerMetadataIdentifier);

            Gson gson = new Gson();
            String data = gson.toJson(serviceParameterMap);
            doStoreConsumerMetadata(consumerMetadataIdentifier, data);
            saveProperties(consumerMetadataIdentifier, data, true, !syncReport);
        } catch (Exception e) {
            // retry again. If failed again, throw exception.
            failedReports.put(consumerMetadataIdentifier, serviceParameterMap);
            metadataReportRetry.startRetryTask();
            logger.error("Failed to put consumer metadata " + consumerMetadataIdentifier + ";  " + serviceParameterMap + ", cause: " + e.getMessage(), e);
        }
    }

    @Override
    public void destroy() {
        if (reportCacheExecutor != null) {
            reportCacheExecutor.shutdown();
        }
        if (reportTimerScheduler != null) {
            reportTimerScheduler.shutdown();
        }
        if (metadataReportRetry != null) {
            metadataReportRetry.destroy();
            metadataReportRetry = null;
        }
    }

    @Override
    public void saveServiceMetadata(ServiceMetadataIdentifier metadataIdentifier, URL url) {
        if (syncReport) {
            doSaveMetadata(metadataIdentifier, url);
        } else {
            reportCacheExecutor.execute(() -> doSaveMetadata(metadataIdentifier, url));
        }
    }

    @Override
    public void removeServiceMetadata(ServiceMetadataIdentifier metadataIdentifier) {
        if (syncReport) {
            doRemoveMetadata(metadataIdentifier);
        } else {
            reportCacheExecutor.execute(() -> doRemoveMetadata(metadataIdentifier));
        }
    }

    @Override
    public List<String> getExportedURLs(ServiceMetadataIdentifier metadataIdentifier) {
        // TODO, fallback to local cache
        return doGetExportedURLs(metadataIdentifier);
    }

    @Override
    public void saveSubscribedData(SubscriberMetadataIdentifier subscriberMetadataIdentifier, Set<String> urls) {
        if (syncReport) {
            doSaveSubscriberData(subscriberMetadataIdentifier, new Gson().toJson(urls));
        } else {
            reportCacheExecutor.execute(() -> doSaveSubscriberData(subscriberMetadataIdentifier, new Gson().toJson(urls)));
        }
    }


    @Override
    public List<String> getSubscribedURLs(SubscriberMetadataIdentifier subscriberMetadataIdentifier) {
        String content = doGetSubscribedURLs(subscriberMetadataIdentifier);
        Type setType = new TypeToken<SortedSet<String>>() {
        }.getType();
        return new Gson().fromJson(content, setType);
    }

    String getProtocol(URL url) {
        String protocol = url.getSide();
        protocol = protocol == null ? url.getProtocol() : protocol;
        return protocol;
    }

    /**
     * @return if need to continue
     */
    public boolean retry() {
        return doHandleMetadataCollection(failedReports);
    }

    @Override
    public boolean shouldReportDefinition() {
        return reportDefinition;
    }

    @Override
    public boolean shouldReportMetadata() {
        return reportMetadata;
    }

    private boolean doHandleMetadataCollection(Map<MetadataIdentifier, Object> metadataMap) {
        if (metadataMap.isEmpty()) {        // 没有上报失败的元数据
            return true;
        }
        // 遍历failedReports集合中失败上报的元数据，逐个调用storeProviderMetadata()方法或storeConsumerMetadata()方法重新上报
        for (Map.Entry<MetadataIdentifier, Object> item : metadataMap.entrySet()) {
            if (PROVIDER_SIDE.equals(item.getKey().getSide())) {
                this.storeProviderMetadata(item.getKey(), (FullServiceDefinition) item.getValue());
            } else if (CONSUMER_SIDE.equals(item.getKey().getSide())) {
                this.storeConsumerMetadata(item.getKey(), (Map) item.getValue());
            }

        }
        return false;
    }

    /**
     * not private. just for unittest.
     */
    void publishAll() {
        logger.info("start to publish all metadata.");
        this.doHandleMetadataCollection(allMetadataReports);
    }

    /**
     * between 2:00 am to 6:00 am, the time is random.
     *
     * @return
     */
    long calculateStartTime() {
        Calendar calendar = Calendar.getInstance();
        long nowMill = calendar.getTimeInMillis();
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        long subtract = calendar.getTimeInMillis() + ONE_DAY_IN_MILLISECONDS - nowMill;
        return subtract + (FOUR_HOURS_IN_MILLISECONDS / 2) + ThreadLocalRandom.current().nextInt(FOUR_HOURS_IN_MILLISECONDS);
    }

    /**
     * 失败重试
     */
    class MetadataReportRetry {
        protected final Logger logger = LoggerFactory.getLogger(getClass());

        // 执行重试任务的线程池
        final ScheduledExecutorService retryExecutor = Executors.newScheduledThreadPool(0, new NamedThreadFactory("DubboMetadataReportRetryTimer", true));
        volatile ScheduledFuture retryScheduledFuture;      // 重试任务关联的 Future 对象
        final AtomicInteger retryCounter = new AtomicInteger(0);        // 记录重试任务的次数
        // retry task schedule period
        long retryPeriod;       // 重试任务的时间间隔
        // if no failed report, wait how many times to run retry task.
        int retryTimesIfNonFail = 600;      // 无失败上报的元数据之后，重试任务会再执行 600 次，才会销毁

        int retryLimit;     // 失败重试的次数上限，默认为100次，即重试失败100次之后会放弃

        public MetadataReportRetry(int retryTimes, int retryPeriod) {
            this.retryPeriod = retryPeriod;
            this.retryLimit = retryTimes;
        }

        void startRetryTask() {
            if (retryScheduledFuture == null) {
                synchronized (retryCounter) {
                    if (retryScheduledFuture == null) {
                        retryScheduledFuture = retryExecutor.scheduleWithFixedDelay(new Runnable() {
                            @Override
                            public void run() {
                                // Check and connect to the metadata
                                try {
                                    int times = retryCounter.incrementAndGet();
                                    logger.info("start to retry task for metadata report. retry times:" + times);
                                    if (retry() && times > retryTimesIfNonFail) {
                                        cancelRetryTask();
                                    }
                                    if (times > retryLimit) {
                                        cancelRetryTask();
                                    }
                                } catch (Throwable t) { // Defensive fault tolerance
                                    logger.error("Unexpected error occur at failed retry, cause: " + t.getMessage(), t);
                                }
                            }
                        }, 500, retryPeriod, TimeUnit.MILLISECONDS);
                    }
                }
            }
        }

        void cancelRetryTask() {
            if (retryScheduledFuture != null) {
                retryScheduledFuture.cancel(false);
            }
            retryExecutor.shutdown();
        }

        void destroy() {
            cancelRetryTask();
        }
    }

    private void doSaveSubscriberData(SubscriberMetadataIdentifier subscriberMetadataIdentifier, List<String> urls) {
        if (CollectionUtils.isEmpty(urls)) {
            return;
        }
        List<String> encodedUrlList = new ArrayList<>(urls.size());
        for (String url : urls) {
            encodedUrlList.add(URL.encode(url));
        }
        doSaveSubscriberData(subscriberMetadataIdentifier, encodedUrlList);
    }

    protected abstract void doStoreProviderMetadata(MetadataIdentifier providerMetadataIdentifier, String serviceDefinitions);

    protected abstract void doStoreConsumerMetadata(MetadataIdentifier consumerMetadataIdentifier, String serviceParameterString);

    protected abstract void doSaveMetadata(ServiceMetadataIdentifier metadataIdentifier, URL url);

    protected abstract void doRemoveMetadata(ServiceMetadataIdentifier metadataIdentifier);

    protected abstract List<String> doGetExportedURLs(ServiceMetadataIdentifier metadataIdentifier);

    protected abstract void doSaveSubscriberData(SubscriberMetadataIdentifier subscriberMetadataIdentifier, String urlListStr);

    protected abstract String doGetSubscribedURLs(SubscriberMetadataIdentifier subscriberMetadataIdentifier);

}
