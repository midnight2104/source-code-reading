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

package org.apache.shenyu.springboot.sync.data.etcd;

import io.etcd.jetcd.Client;
import lombok.extern.slf4j.Slf4j;
import org.apache.shenyu.sync.data.api.AuthDataSubscriber;
import org.apache.shenyu.sync.data.api.MetaDataSubscriber;
import org.apache.shenyu.sync.data.api.PluginDataSubscriber;
import org.apache.shenyu.sync.data.api.SyncDataService;
import org.apache.shenyu.sync.data.etcd.EtcdClient;
import org.apache.shenyu.sync.data.etcd.EtcdSyncDataService;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Collections;
import java.util.List;

/**
 * Etcd sync data configuration for spring boot.
 * etcd 数据同步配置
 */
@Configuration
@ConditionalOnClass(EtcdSyncDataConfiguration.class)
@ConditionalOnProperty(prefix = "shenyu.sync.etcd", name = "url")
@EnableConfigurationProperties(EtcdConfig.class)
@Slf4j
public class EtcdSyncDataConfiguration {

    /**
     * Sync data service sync data service.
     *
     * @param etcdClients          the etcd client
     * @param pluginSubscriber the plugin subscriber
     * @param metaSubscribers   the meta subscribers
     * @param authSubscribers   the auth subscribers
     * @return the sync data service
     */
    @Bean
    public SyncDataService syncDataService(final ObjectProvider<EtcdClient> etcdClients,
                                           final ObjectProvider<PluginDataSubscriber> pluginSubscriber,
                                           final ObjectProvider<List<MetaDataSubscriber>> metaSubscribers,
                                           final ObjectProvider<List<AuthDataSubscriber>> authSubscribers) {
        log.info("you use etcd sync shenyu data.......");
        return new EtcdSyncDataService(etcdClients.getIfAvailable(), pluginSubscriber.getIfAvailable(),
                metaSubscribers.getIfAvailable(Collections::emptyList), authSubscribers.getIfAvailable(Collections::emptyList));
    }

    /**
     * register etcd Client in spring ioc.
     *
     * @param etcdConfig the etcd configuration
     * @return EtcdClient {@linkplain EtcdClient}
     */
    @Bean
    public EtcdClient etcdClient(final EtcdConfig etcdConfig) {
        Client client = Client.builder()
                .endpoints(etcdConfig.getUrl())
                .build();
        return new EtcdClient(client);
    }
}
