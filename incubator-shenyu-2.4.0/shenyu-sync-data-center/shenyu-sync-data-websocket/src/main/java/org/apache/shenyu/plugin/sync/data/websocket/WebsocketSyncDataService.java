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

package org.apache.shenyu.plugin.sync.data.websocket;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.shenyu.common.concurrent.ShenyuThreadFactory;
import org.apache.shenyu.plugin.sync.data.websocket.client.ShenyuWebsocketClient;
import org.apache.shenyu.plugin.sync.data.websocket.config.WebsocketConfig;
import org.apache.shenyu.sync.data.api.AuthDataSubscriber;
import org.apache.shenyu.sync.data.api.MetaDataSubscriber;
import org.apache.shenyu.sync.data.api.PluginDataSubscriber;
import org.apache.shenyu.sync.data.api.SyncDataService;
import org.java_websocket.client.WebSocketClient;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Websocket数据同步服务
 * Websocket sync data service.
 */
@Slf4j
public class WebsocketSyncDataService implements SyncDataService, AutoCloseable {

    private final List<WebSocketClient> clients = new ArrayList<>();

    private final ScheduledThreadPoolExecutor executor;

    /**
     * Instantiates a new Websocket sync cache.
     *
     * @param websocketConfig      the websocket config
     * @param pluginDataSubscriber the plugin data subscriber
     * @param metaDataSubscribers  the meta data subscribers
     * @param authDataSubscribers  the auth data subscribers
     */
    public WebsocketSyncDataService(final WebsocketConfig websocketConfig,
                                    final PluginDataSubscriber pluginDataSubscriber,
                                    final List<MetaDataSubscriber> metaDataSubscribers,
                                    final List<AuthDataSubscriber> authDataSubscribers) {
        // admin端的同步地址，有多个的话，使用","分割
        String[] urls = StringUtils.split(websocketConfig.getUrls(), ",");
        // 创建调度线程池，一个admin分配一个
        executor = new ScheduledThreadPoolExecutor(urls.length, ShenyuThreadFactory.create("websocket-connect", true));
        for (String url : urls) {
            try {
                //创建WebsocketClient，一个admin分配一个
                clients.add(new ShenyuWebsocketClient(new URI(url), Objects.requireNonNull(pluginDataSubscriber), metaDataSubscribers, authDataSubscribers));
            } catch (URISyntaxException e) {
                log.error("websocket url({}) is error", url, e);
            }
        }
        try {
            for (WebSocketClient client : clients) {
                // 和websocket server建立连接
                boolean success = client.connectBlocking(3000, TimeUnit.MILLISECONDS);
                if (success) {
                    log.info("websocket connection is successful.....");
                } else {
                    log.error("websocket connection is error.....");
                }

                // 执行定时任务，每隔10秒执行一次
                // 主要作用是判断websocket连接是否已经断开，如果已经断开，则尝试重连。
                // 如果没有断开，就进行 ping-pong 检测
                executor.scheduleAtFixedRate(() -> {
                    try {
                        if (client.isClosed()) {
                            boolean reconnectSuccess = client.reconnectBlocking();
                            if (reconnectSuccess) {
                                log.info("websocket reconnect server[{}] is successful.....", client.getURI().toString());
                            } else {
                                log.error("websocket reconnection server[{}] is error.....", client.getURI().toString());
                            }
                        } else {
                            client.sendPing();
                            log.debug("websocket send to [{}] ping message successful", client.getURI().toString());
                        }
                    } catch (InterruptedException e) {
                        log.error("websocket connect is error :{}", e.getMessage());
                    }
                }, 10, 10, TimeUnit.SECONDS);
            }
            /* client.setProxy(new Proxy(Proxy.Type.HTTP, new InetSocketAddress("proxyaddress", 80)));*/
        } catch (InterruptedException e) {
            log.info("websocket connection...exception....", e);
        }

    }

    @Override
    public void close() {
        // 关闭 websocket client
        for (WebSocketClient client : clients) {
            if (!client.isClosed()) {
                client.close();
            }
        }
        // 关闭线程池
        if (Objects.nonNull(executor)) {
            executor.shutdown();
        }
    }
}
