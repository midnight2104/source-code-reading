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

package org.apache.shenyu.sync.data.nacos.handler;

import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.config.listener.Listener;
import com.google.common.collect.Maps;
import com.google.gson.JsonParseException;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.shenyu.common.constant.NacosPathConstants;
import org.apache.shenyu.common.dto.AppAuthData;
import org.apache.shenyu.common.dto.MetaData;
import org.apache.shenyu.common.dto.PluginData;
import org.apache.shenyu.common.dto.RuleData;
import org.apache.shenyu.common.dto.SelectorData;
import org.apache.shenyu.common.utils.GsonUtils;
import org.apache.shenyu.sync.data.api.AuthDataSubscriber;
import org.apache.shenyu.sync.data.api.MetaDataSubscriber;
import org.apache.shenyu.sync.data.api.PluginDataSubscriber;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

/**
 * Nacos cache handler.
 */
@Slf4j
public class NacosCacheHandler {

    protected static final Map<String, List<Listener>> LISTENERS = Maps.newConcurrentMap();

    @Getter
    private final ConfigService configService;

    private final PluginDataSubscriber pluginDataSubscriber;

    private final List<MetaDataSubscriber> metaDataSubscribers;

    private final List<AuthDataSubscriber> authDataSubscribers;

    public NacosCacheHandler(final ConfigService configService, final PluginDataSubscriber pluginDataSubscriber,
                             final List<MetaDataSubscriber> metaDataSubscribers,
                             final List<AuthDataSubscriber> authDataSubscribers) {
        this.configService = configService;
        this.pluginDataSubscriber = pluginDataSubscriber;
        this.metaDataSubscribers = metaDataSubscribers;
        this.authDataSubscribers = authDataSubscribers;
    }

    protected void updatePluginMap(final String configInfo) {
        try {
            // Fix bug #656(https://github.com/dromara/shenyu/issues/656)
            List<PluginData> pluginDataList = new ArrayList<>(GsonUtils.getInstance().toObjectMap(configInfo, PluginData.class).values());
            pluginDataList.forEach(pluginData -> Optional.ofNullable(pluginDataSubscriber).ifPresent(subscriber -> {
                subscriber.unSubscribe(pluginData);
                subscriber.onSubscribe(pluginData);
            }));
        } catch (JsonParseException e) {
            log.error("sync plugin data have error:", e);
        }
    }

    /**
     * ?????????????????????
     * @param configInfo ?????????????????????
     */
    protected void updateSelectorMap(final String configInfo) {
        try {
            List<SelectorData> selectorDataList = GsonUtils.getInstance().toObjectMapList(configInfo, SelectorData.class).values().stream().flatMap(Collection::stream).collect(Collectors.toList());
            selectorDataList.forEach(selectorData -> Optional.ofNullable(pluginDataSubscriber).ifPresent(subscriber -> {
                //?????????
                subscriber.unSelectorSubscribe(selectorData);
                //?????????
                subscriber.onSelectorSubscribe(selectorData);
            }));
        } catch (JsonParseException e) {
            log.error("sync selector data have error:", e);
        }
    }

    protected void updateRuleMap(final String configInfo) {
        try {
            List<RuleData> ruleDataList = GsonUtils.getInstance().toObjectMapList(configInfo, RuleData.class).values()
                    .stream().flatMap(Collection::stream)
                    .collect(Collectors.toList());
            ruleDataList.forEach(ruleData -> Optional.ofNullable(pluginDataSubscriber).ifPresent(subscriber -> {
                subscriber.unRuleSubscribe(ruleData);
                subscriber.onRuleSubscribe(ruleData);
            }));
        } catch (JsonParseException e) {
            log.error("sync rule data have error:", e);
        }
    }

    protected void updateMetaDataMap(final String configInfo) {
        try {
            List<MetaData> metaDataList = new ArrayList<>(GsonUtils.getInstance().toObjectMap(configInfo, MetaData.class).values());
            metaDataList.forEach(metaData -> metaDataSubscribers.forEach(subscriber -> {
                subscriber.unSubscribe(metaData);
                subscriber.onSubscribe(metaData);
            }));
        } catch (JsonParseException e) {
            log.error("sync meta data have error:", e);
        }
    }

    protected void updateAuthMap(final String configInfo) {
        try {
            List<AppAuthData> appAuthDataList = new ArrayList<>(GsonUtils.getInstance().toObjectMap(configInfo, AppAuthData.class).values());
            appAuthDataList.forEach(appAuthData -> authDataSubscribers.forEach(subscriber -> {
                subscriber.unSubscribe(appAuthData);
                subscriber.onSubscribe(appAuthData);
            }));
        } catch (JsonParseException e) {
            log.error("sync auth data have error:", e);
        }
    }

    @SneakyThrows
    private String getConfigAndSignListener(final String dataId, final Listener listener) {
        String config = configService.getConfigAndSignListener(dataId, NacosPathConstants.GROUP, 6000, listener);
        if (config == null) {
            config = "{}";
        }
        return config;
    }

    protected void watcherData(final String dataId, final OnChange oc) {
        // ???????????????
        Listener listener = new Listener() {
            @Override
            public void receiveConfigInfo(final String configInfo) {
                // ?????????????????????????????????????????????
                oc.change(configInfo);
            }

            @Override
            public Executor getExecutor() {
                return null;
            }
        };

        // ????????????????????????
        oc.change(getConfigAndSignListener(dataId, listener));
        LISTENERS.computeIfAbsent(dataId, key -> new ArrayList<>()).add(listener);
    }

    protected interface OnChange {

        void change(String changeData);
    }
}
