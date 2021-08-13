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

package org.apache.shenyu.admin.listener.http;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.shenyu.admin.config.properties.HttpSyncProperties;
import org.apache.shenyu.admin.utils.ShenyuResultMessage;
import org.apache.shenyu.admin.listener.AbstractDataChangedListener;
import org.apache.shenyu.admin.listener.ConfigDataCache;
import org.apache.shenyu.admin.model.result.ShenyuAdminResult;
import org.apache.shenyu.common.concurrent.ShenyuThreadFactory;
import org.apache.shenyu.common.constant.HttpConstants;
import org.apache.shenyu.common.dto.AppAuthData;
import org.apache.shenyu.common.dto.MetaData;
import org.apache.shenyu.common.dto.PluginData;
import org.apache.shenyu.common.dto.RuleData;
import org.apache.shenyu.common.dto.SelectorData;
import org.apache.shenyu.common.enums.ConfigGroupEnum;
import org.apache.shenyu.common.enums.DataEventTypeEnum;
import org.apache.shenyu.common.exception.ShenyuException;
import org.apache.shenyu.common.utils.GsonUtils;
import org.springframework.http.MediaType;

import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * HTTP long polling, which blocks the client's request thread
 * and informs the client of group information about data changes
 * when there are data changes. If there is no data change after the specified time,
 * the client will make a listening request again.
 *
 * @since 2.0.0
 */
@Slf4j
@SuppressWarnings("all")
public class HttpLongPollingDataChangedListener extends AbstractDataChangedListener {

    private static final String X_REAL_IP = "X-Real-IP";

    private static final String X_FORWARDED_FOR = "X-Forwarded-For";

    private static final String X_FORWARDED_FOR_SPLIT_SYMBOL = ",";

    private static final ReentrantLock LOCK = new ReentrantLock();

    /**
     * Blocked client.
     */
    private final BlockingQueue<LongPollingClient> clients;

    private final ScheduledExecutorService scheduler;

    private final HttpSyncProperties httpSyncProperties;

    /**
     * Instantiates a new Http long polling data changed listener.
     * @param httpSyncProperties the HttpSyncProperties
     */
    public HttpLongPollingDataChangedListener(final HttpSyncProperties httpSyncProperties) {
        // 默认客户端（这里是网关）1024个
        this.clients = new ArrayBlockingQueue<>(1024);
        // 创建线程池
        // ScheduledThreadPoolExecutor 可以执行延迟任务，周期任务，普通任务
        this.scheduler = new ScheduledThreadPoolExecutor(1,
                ShenyuThreadFactory.create("long-polling", true));
        // 长轮询的属性信息
        this.httpSyncProperties = httpSyncProperties;
    }

    /**
     * 在 InitializingBean接口中的afterPropertiesSet()方法中被调用，即在bean的初始化过程中执行
     */
    @Override
    protected void afterInitialize() {
        long syncInterval = httpSyncProperties.getRefreshInterval().toMillis();
        // Periodically check the data for changes and update the cache

        // 执行周期任务：更新内存中（CACHE）的数据每隔5分钟执行一次，5分钟后开始执行
        // 防止admin先启动一段时间后，产生了数据；然后网关初次连接时，没有拿到全量数据
        scheduler.scheduleWithFixedDelay(() -> {
            log.info("http sync strategy refresh config start.");
            try {
                // 从数据库读取数据到本地缓存（这里就是内存）
                this.refreshLocalCache();
                log.info("http sync strategy refresh config success.");
            } catch (Exception e) {
                log.error("http sync strategy refresh config error!", e);
            }
        }, syncInterval, syncInterval, TimeUnit.MILLISECONDS);
        log.info("http sync strategy refresh interval: {}ms", syncInterval);
    }

    // 从数据库读取数据到本地缓存（这里就是内存）
    private void refreshLocalCache() {
        this.updateAppAuthCache();
        this.updatePluginCache();
        this.updateRuleCache();
        this.updateSelectorCache();
        this.updateMetaDataCache();
    }

    /**
     * If the configuration data changes, the group information for the change is immediately responded.
     * Otherwise, the client's request thread is blocked until any data changes or the specified timeout is reached.
     *
     * @param request  the request
     * @param response the response
     */
    /**
     * 执行长轮询：如果有配置信息变更，这个配置信息将会立即响应给客户端（这里就是网关端）。
     * 否则，否则客户端会一直被阻塞，直到有数据变更或者超时。
     * @param request
     * @param response
     */
    public void doLongPolling(final HttpServletRequest request, final HttpServletResponse response) {
        // compare group md5
        // 比较md5，判断网关的数据和admin端的数据是否一致，得到发生变更的数据
        List<ConfigGroupEnum> changedGroup = compareChangedGroup(request);
        String clientIp = getRemoteIp(request);
        // response immediately.
        // 有变更的数据，则立即向网关响应
        if (CollectionUtils.isNotEmpty(changedGroup)) {
            this.generateResponse(response, changedGroup);
            log.info("send response with the changed group, ip={}, group={}", clientIp, changedGroup);
            return;
        }

         // 没有变更，则将客户端（这里就是网关）放进阻塞队列
        // listen for configuration changed.
        final AsyncContext asyncContext = request.startAsync();
        // AsyncContext.settimeout() does not timeout properly, so you have to control it yourself
        asyncContext.setTimeout(0L);
        // block client's thread.
        scheduler.execute(new LongPollingClient(asyncContext, clientIp, HttpConstants.SERVER_MAX_HOLD_TIMEOUT));
    }

    @Override
    protected void afterAppAuthChanged(final List<AppAuthData> changed, final DataEventTypeEnum eventType) {
        // 在线程池中执行
        scheduler.execute(new DataChangeTask(ConfigGroupEnum.APP_AUTH));
    }

    @Override
    protected void afterMetaDataChanged(final List<MetaData> changed, final DataEventTypeEnum eventType) {
        // 在线程池中执行
        scheduler.execute(new DataChangeTask(ConfigGroupEnum.META_DATA));
    }

    @Override
    protected void afterPluginChanged(final List<PluginData> changed, final DataEventTypeEnum eventType) {
        // 在线程池中执行
        scheduler.execute(new DataChangeTask(ConfigGroupEnum.PLUGIN));
    }

    @Override
    protected void afterRuleChanged(final List<RuleData> changed, final DataEventTypeEnum eventType) {
        // 在线程池中执行
        scheduler.execute(new DataChangeTask(ConfigGroupEnum.RULE));
    }

    @Override
    protected void afterSelectorChanged(final List<SelectorData> changed, final DataEventTypeEnum eventType) {
        // 在线程池中执行
        scheduler.execute(new DataChangeTask(ConfigGroupEnum.SELECTOR));
    }

    /**
     * 判断组数据是否发生变更
     * @param request
     * @return
     */
    private List<ConfigGroupEnum> compareChangedGroup(final HttpServletRequest request) {
        List<ConfigGroupEnum> changedGroup = new ArrayList<>(ConfigGroupEnum.values().length);
        for (ConfigGroupEnum group : ConfigGroupEnum.values()) {
            // md5,lastModifyTime
            String[] params = StringUtils.split(request.getParameter(group.name()), ',');
            if (params == null || params.length != 2) {
                throw new ShenyuException("group param invalid:" + request.getParameter(group.name()));
            }
            String clientMd5 = params[0];
            long clientModifyTime = NumberUtils.toLong(params[1]);
            ConfigDataCache serverCache = CACHE.get(group.name());
            // do check.
            if (this.checkCacheDelayAndUpdate(serverCache, clientMd5, clientModifyTime)) {
                changedGroup.add(group);
            }
        }
        return changedGroup;
    }

    /**
     * check whether the client needs to update the cache.
     * @param serverCache the admin local cache
     * @param clientMd5 the client md5 value
     * @param clientModifyTime the client last modify time
     * @return true: the client needs to be updated, false: not need.
     */
    private boolean checkCacheDelayAndUpdate(final ConfigDataCache serverCache, final String clientMd5, final long clientModifyTime) {
        // is the same, doesn't need to be updated
        if (StringUtils.equals(clientMd5, serverCache.getMd5())) {
            return false;
        }
        // if the md5 value is different, it is necessary to compare lastModifyTime.
        long lastModifyTime = serverCache.getLastModifyTime();
        if (lastModifyTime >= clientModifyTime) {
            // the client's config is out of date.
            return true;
        }
        // the lastModifyTime before client, then the local cache needs to be updated.
        // Considering the concurrency problem, admin must lock,
        // otherwise it may cause the request from shenyu-web to update the cache concurrently, causing excessive db pressure
        boolean locked = false;
        try {
            locked = LOCK.tryLock(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return true;
        }
        if (locked) {
            try {
                ConfigDataCache latest = CACHE.get(serverCache.getGroup());
                if (latest != serverCache) {
                    // the cache of admin was updated. if the md5 value is the same, there's no need to update.
                    return !StringUtils.equals(clientMd5, latest.getMd5());
                }
                // load cache from db.
                this.refreshLocalCache();
                latest = CACHE.get(serverCache.getGroup());
                return !StringUtils.equals(clientMd5, latest.getMd5());
            } finally {
                LOCK.unlock();
            }
        }
        // not locked, the client need to be updated.
        return true;
    }

    /**
     * Send response datagram.
     *
     * @param response      the response
     * @param changedGroups the changed groups
     */
    private void generateResponse(final HttpServletResponse response, final List<ConfigGroupEnum> changedGroups) {
        try {
            response.setHeader("Pragma", "no-cache");
            response.setDateHeader("Expires", 0);
            response.setHeader("Cache-Control", "no-cache,no-store");
            response.setContentType(MediaType.APPLICATION_JSON_VALUE);
            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().println(GsonUtils.getInstance().toJson(ShenyuAdminResult.success(ShenyuResultMessage.SUCCESS, changedGroups)));
        } catch (IOException ex) {
            log.error("Sending response failed.", ex);
        }
    }

    /**
     * get real client ip.
     *
     * @param request the request
     * @return the remote ip
     */
    private static String getRemoteIp(final HttpServletRequest request) {
        String xForwardedFor = request.getHeader(X_FORWARDED_FOR);
        if (!StringUtils.isBlank(xForwardedFor)) {
            return xForwardedFor.split(X_FORWARDED_FOR_SPLIT_SYMBOL)[0].trim();
        }
        String header = request.getHeader(X_REAL_IP);
        return StringUtils.isBlank(header) ? request.getRemoteAddr() : header;
    }

    /**
     * When a group's data changes, the thread is created to notify the client asynchronously.
     */
    class DataChangeTask implements Runnable {

        /**
         * The Group where the data has changed.
         */
        private final ConfigGroupEnum groupKey;

        /**
         * The Change time.
         */
        private final long changeTime = System.currentTimeMillis();

        /**
         * Instantiates a new Data change task.
         *
         * @param groupKey the group key
         */
        DataChangeTask(final ConfigGroupEnum groupKey) {
            this.groupKey = groupKey;
        }

        @Override
        public void run() {
            // 阻塞队列中的客户端超过了给定的值，则分批执行
            if (clients.size() > httpSyncProperties.getNotifyBatchSize()) {
                List<LongPollingClient> targetClients = new ArrayList<>(clients.size());
                clients.drainTo(targetClients);
                List<List<LongPollingClient>> partitionClients = Lists.partition(targetClients, httpSyncProperties.getNotifyBatchSize());
               // 分批执行
                partitionClients.forEach(item -> scheduler.execute(() -> doRun(item)));
            } else {
                // 执行任务
                doRun(clients);
            }
        }

        private void doRun(final Collection<LongPollingClient> clients) {
            // 通知所有客户端发生了数据变更
            for (Iterator<LongPollingClient> iter = clients.iterator(); iter.hasNext();) {
                LongPollingClient client = iter.next();
                iter.remove();
                // 发送响应
                client.sendResponse(Collections.singletonList(groupKey));
                log.info("send response with the changed group,ip={}, group={}, changeTime={}", client.ip, groupKey, changeTime);
            }
        }
    }

    /**
     * If you exceed {@link HttpConstants#SERVER_MAX_HOLD_TIMEOUT} and still have no data change,
     * empty data is returned. If the data changes within this time frame, the DataChangeTask
     * cancellations the timed task and responds to the changed group data.
     */
    class LongPollingClient implements Runnable {

        /**
         * The Async context.
         */
        private final AsyncContext asyncContext;

        /**
         * The Ip.
         */
        private final String ip;

        /**
         * The Timeout time.
         */
        private final long timeoutTime;

        /**
         * The Async timeout future.
         */
        private Future<?> asyncTimeoutFuture;

        /**
         * Instantiates a new Long polling client.
         *
         * @param ac          the ac
         * @param ip          the ip
         * @param timeoutTime the timeout time
         */
        LongPollingClient(final AsyncContext ac, final String ip, final long timeoutTime) {
            this.asyncContext = ac;
            this.ip = ip;
            this.timeoutTime = timeoutTime;
        }

        @Override
        public void run() {
            try {
                // 60秒后移除，并响应客户端
                this.asyncTimeoutFuture = scheduler.schedule(() -> {
                    clients.remove(LongPollingClient.this);
                    List<ConfigGroupEnum> changedGroups = compareChangedGroup((HttpServletRequest) asyncContext.getRequest());
                    sendResponse(changedGroups);
                }, timeoutTime, TimeUnit.MILLISECONDS);

                // 添加到阻塞队列
                clients.add(this);

            } catch (Exception ex) {
                log.error("add long polling client error", ex);
            }
        }

        /**
         * Send response.
         *
         * @param changedGroups the changed groups
         */
        void sendResponse(final List<ConfigGroupEnum> changedGroups) {
            // cancel scheduler
            if (null != asyncTimeoutFuture) {
                asyncTimeoutFuture.cancel(false);
            }
            // 响应变更的组
            generateResponse((HttpServletResponse) asyncContext.getResponse(), changedGroups);
            asyncContext.complete();
        }
    }
}
