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

package org.apache.shenyu.admin.listener.consul;

import com.ecwid.consul.v1.ConsulClient;
import lombok.SneakyThrows;
import org.apache.shenyu.admin.service.SyncDataService;
import org.apache.shenyu.common.constant.ConsulConstants;
import org.apache.shenyu.common.enums.DataEventTypeEnum;
import org.springframework.boot.CommandLineRunner;

/**
 * The type Consul data init.
 * 使用Consul进行数据同步时，数据初始化操作
 * 在应用程序启动后执行
 * 接口 consulClient.setKVValue() 向consul中存值
 * 接口 consulClient.getKVValue() 从consul中获取值
 *
 */
public class ConsulDataInit implements CommandLineRunner {
    private final ConsulClient consulClient;

    private final SyncDataService syncDataService;

    /**
     * Instantiates a new Consul data init.
     * @param consulClient the Consul client
     * @param syncDataService the sync data service
     */
    public ConsulDataInit(final ConsulClient consulClient, final SyncDataService syncDataService) {
        this.consulClient = consulClient;
        this.syncDataService = syncDataService;
    }

    @Override
    public void run(final String... args) {
        String pluginData = ConsulConstants.PLUGIN_DATA;
        String authData = ConsulConstants.AUTH_DATA;
        String metaData = ConsulConstants.META_DATA;
        // 第一次，数据不存在consul中，进行全量同步
        if (dataKeyNotExist(pluginData) && dataKeyNotExist(authData) && dataKeyNotExist(metaData)) {
            syncDataService.syncAll(DataEventTypeEnum.REFRESH);
        }
    }

    @SneakyThrows
    private boolean dataKeyNotExist(final String dataKey) {
        return consulClient.getKVValue(dataKey).getValue() == null;
    }
}
