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

package org.apache.shenyu.sync.data.etcd;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.shenyu.common.constant.DefaultPathConstants;
import org.apache.shenyu.common.dto.MetaData;
import org.apache.shenyu.common.dto.PluginData;
import org.apache.shenyu.common.dto.SelectorData;
import org.apache.shenyu.common.dto.RuleData;
import org.apache.shenyu.common.dto.AppAuthData;
import org.apache.shenyu.common.enums.ConfigGroupEnum;
import org.apache.shenyu.common.utils.CollectionUtils;
import org.apache.shenyu.common.utils.GsonUtils;
import org.apache.shenyu.sync.data.api.AuthDataSubscriber;
import org.apache.shenyu.sync.data.api.MetaDataSubscriber;
import org.apache.shenyu.sync.data.api.PluginDataSubscriber;
import org.apache.shenyu.sync.data.api.SyncDataService;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

/**
 * Data synchronize of etcd.
 * 使用etcd进行数据同步
 */
@Slf4j
public class EtcdSyncDataService implements SyncDataService, AutoCloseable {

    private final EtcdClient etcdClient;

    private final PluginDataSubscriber pluginDataSubscriber;

    private final List<MetaDataSubscriber> metaDataSubscribers;

    private final List<AuthDataSubscriber> authDataSubscribers;

    /**
     * Instantiates a new Zookeeper cache manager.
     * 实例化etcd缓存管理器
     * @param etcdClient             the etcd client
     * @param pluginDataSubscriber the plugin data subscriber
     * @param metaDataSubscribers  the meta data subscribers
     * @param authDataSubscribers  the auth data subscribers
     */
    public EtcdSyncDataService(final EtcdClient etcdClient, final PluginDataSubscriber pluginDataSubscriber,
                                    final List<MetaDataSubscriber> metaDataSubscribers, final List<AuthDataSubscriber> authDataSubscribers) {
        this.etcdClient = etcdClient;
        this.pluginDataSubscriber = pluginDataSubscriber;
        this.metaDataSubscribers = metaDataSubscribers;
        this.authDataSubscribers = authDataSubscribers;
        // 监听插件、选择器、规则信息
        watcherData();
        // 监听 认证信息
        watchAppAuth();
        // 监听元数据
        watchMetaData();
    }

    private void watcherData() {
        final String pluginParent = DefaultPathConstants.PLUGIN_PARENT;
        // 获取所有插件
        List<String> pluginZKs = etcdClientGetChildren(pluginParent);
        for (String pluginName : pluginZKs) {
            // 缓存数据并订阅节点：数据内容发生变更
            watcherAll(pluginName);
        }

        // 订阅节点信息：新增或删除节点
        etcdClient.watchChildChange(pluginParent, (updateNode, updateValue) -> {
            if (!updateNode.isEmpty()) {
                watcherAll(updateNode);
            }
        }, null);
    }

    private void watcherAll(final String pluginName) {
        watcherPlugin(pluginName);
        watcherSelector(pluginName);
        watcherRule(pluginName);
    }

    private void watcherPlugin(final String pluginName) {
        String pluginPath = DefaultPathConstants.buildPluginPath(pluginName);
        // 从etcd获取数据缓存到网关的内存中
        cachePluginData(etcdClient.get(pluginPath));
        // 订阅插件数据变更节点
        subscribePluginDataChanges(pluginPath, pluginName);
    }

    private void watcherSelector(final String pluginName) {
        // 选择器节点
        String selectorParentPath = DefaultPathConstants.buildSelectorParentPath(pluginName);
        // 选择器子节点
        List<String> childrenList = etcdClientGetChildren(selectorParentPath);
        if (CollectionUtils.isNotEmpty(childrenList)) {
            // 处理每一个选择器
            childrenList.forEach(children -> {
                String realPath = buildRealPath(selectorParentPath, children);
                // 获取etcd中的数据，缓存到网关内存中
                cacheSelectorData(etcdClient.get(realPath));
                // 订阅当前节点，如果数据有更新则执行更新操作
                subscribeSelectorDataChanges(realPath);
            });
        }

        // 订阅节点信息：新增或删除节点
        subscribeChildChanges(ConfigGroupEnum.SELECTOR, selectorParentPath);
    }

    private void watcherRule(final String pluginName) {
        String ruleParent = DefaultPathConstants.buildRuleParentPath(pluginName);
        List<String> childrenList = etcdClientGetChildren(ruleParent);

        if (CollectionUtils.isNotEmpty(childrenList)) {
            childrenList.forEach(children -> {
                String realPath = buildRealPath(ruleParent, children);
                // 从etcd获取数据并缓存到网关内存
                cacheRuleData(etcdClient.get(realPath));
                // 订阅数据变更
                subscribeRuleDataChanges(realPath);
            });
        }
        // 订阅节点变更
        subscribeChildChanges(ConfigGroupEnum.RULE, ruleParent);
    }

    private void watchAppAuth() {
        final String appAuthParent = DefaultPathConstants.APP_AUTH_PARENT;
        List<String> childrenList = etcdClientGetChildren(appAuthParent);
        if (CollectionUtils.isNotEmpty(childrenList)) {
            childrenList.forEach(children -> {
                String realPath = buildRealPath(appAuthParent, children);
                // 从etcd获取数据并缓存到网关内存
                cacheAuthData(etcdClient.get(realPath));
                // 订阅数据变更
                subscribeAppAuthDataChanges(realPath);
            });
        }
        // 订阅节点变更
        subscribeChildChanges(ConfigGroupEnum.APP_AUTH, appAuthParent);
    }

    private void watchMetaData() {
        final String metaDataPath = DefaultPathConstants.META_DATA;
        List<String> childrenList = etcdClientGetChildren(metaDataPath);
        if (CollectionUtils.isNotEmpty(childrenList)) {
            childrenList.forEach(children -> {
                String realPath = buildRealPath(metaDataPath, children);
                // 从etcd获取数据并缓存到网关内存
                cacheMetaData(etcdClient.get(realPath));
                // 订阅数据变更
                subscribeMetaDataChanges(realPath);
            });
        }
        // 订阅节点变更
        subscribeChildChanges(ConfigGroupEnum.META_DATA, metaDataPath);
    }

    private void subscribeChildChanges(final ConfigGroupEnum groupKey, final String groupParentPath) {
        switch (groupKey) {
            case SELECTOR:
                etcdClient.watchChildChange(groupParentPath, (updatePath, updateValue) -> {
                    cacheSelectorData(etcdClient.get(updatePath));
                    subscribeSelectorDataChanges(updatePath);
                }, null);
                break;
            case RULE:
                etcdClient.watchChildChange(groupParentPath, (updatePath, updateValue) -> {
                    cacheRuleData(etcdClient.get(updatePath));
                    subscribeRuleDataChanges(updatePath);
                }, null);
                break;
            case APP_AUTH:
                etcdClient.watchChildChange(groupParentPath, (updatePath, updateValue) -> {
                    cacheAuthData(etcdClient.get(updatePath));
                    subscribeAppAuthDataChanges(updatePath);
                }, null);
                break;
            case META_DATA:
                etcdClient.watchChildChange(groupParentPath, (updatePath, updateValue) -> {
                    cacheMetaData(etcdClient.get(updatePath));
                    subscribeMetaDataChanges(updatePath);
                }, null);
                break;
            default:
                throw new IllegalStateException("Unexpected groupKey: " + groupKey);
        }
    }

    private void subscribePluginDataChanges(final String pluginPath, final String pluginName) {
        // 为key添加更新和删除事件处理器
        etcdClient.watchDataChange(pluginPath, (updatePath, updateValue) -> {
            final String dataPath = buildRealPath(pluginPath, updatePath);
            final String dataStr = etcdClient.get(dataPath);
            final PluginData data = GsonUtils.getInstance().fromJson(dataStr, PluginData.class);
            Optional.ofNullable(data)
                    .ifPresent(d -> Optional.ofNullable(pluginDataSubscriber).ifPresent(e -> e.onSubscribe(d)));
        }, deleteNode -> deletePlugin(pluginName));
    }

    private void deletePlugin(final String pluginName) {
        final PluginData data = new PluginData();
        data.setName(pluginName);
        Optional.ofNullable(pluginDataSubscriber).ifPresent(e -> e.unSubscribe(data));
    }

    private void subscribeSelectorDataChanges(final String path) {
        etcdClient.watchDataChange(path, (updateNode, updateValue) -> cacheSelectorData(updateValue),
                this::unCacheSelectorData);
    }

    private void subscribeRuleDataChanges(final String path) {
        etcdClient.watchDataChange(path, (updatePath, updateValue) -> cacheRuleData(updateValue),
                this::unCacheRuleData);
    }

    private void subscribeAppAuthDataChanges(final String realPath) {
        etcdClient.watchDataChange(realPath, (updatePath, updateValue) -> cacheAuthData(updateValue),
                this::unCacheAuthData);
    }

    private void subscribeMetaDataChanges(final String realPath) {
        etcdClient.watchDataChange(realPath, (updatePath, updateValue) -> cacheMetaData(updateValue),
                this::deleteMetaData);
    }

    private void deleteMetaData(final String deletePath) {
        final String path = deletePath.substring(DefaultPathConstants.META_DATA.length() + 1);
        MetaData metaData = new MetaData();

        try {
            metaData.setPath(URLDecoder.decode(path, StandardCharsets.UTF_8.name()));
            unCacheMetaData(metaData);
            etcdClient.watchClose(path);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    private void cachePluginData(final String dataString) {
        final PluginData pluginData = GsonUtils.getInstance().fromJson(dataString, PluginData.class);
        Optional.ofNullable(pluginData)
                .flatMap(data -> Optional.ofNullable(pluginDataSubscriber)).ifPresent(e -> e.onSubscribe(pluginData));
    }

    private void cacheSelectorData(final String dataString) {
        final SelectorData selectorData = GsonUtils.getInstance().fromJson(dataString, SelectorData.class);
        Optional.ofNullable(selectorData)
                .ifPresent(data -> Optional.ofNullable(pluginDataSubscriber).ifPresent(e -> e.onSelectorSubscribe(data)));
    }

    private void unCacheSelectorData(final String dataPath) {
        SelectorData selectorData = new SelectorData();
        final String selectorId = dataPath.substring(dataPath.lastIndexOf("/") + 1);
        final String str = dataPath.substring(DefaultPathConstants.SELECTOR_PARENT.length());
        final String pluginName = str.substring(1, str.length() - selectorId.length() - 1);
        selectorData.setPluginName(pluginName);
        selectorData.setId(selectorId);
        Optional.ofNullable(pluginDataSubscriber).ifPresent(e -> e.unSelectorSubscribe(selectorData));
        etcdClient.watchClose(dataPath);
    }

    private void cacheRuleData(final String dataString) {
        final RuleData ruleData = GsonUtils.getInstance().fromJson(dataString, RuleData.class);
        Optional.ofNullable(ruleData)
                .ifPresent(data -> Optional.ofNullable(pluginDataSubscriber).ifPresent(e -> e.onRuleSubscribe(data)));
    }

    private void unCacheRuleData(final String dataPath) {
        String substring = dataPath.substring(dataPath.lastIndexOf("/") + 1);
        final String str = dataPath.substring(DefaultPathConstants.RULE_PARENT.length());
        final String pluginName = str.substring(1, str.length() - substring.length() - 1);
        final List<String> list = Lists.newArrayList(Splitter.on(DefaultPathConstants.SELECTOR_JOIN_RULE).split(substring));
        RuleData ruleData = new RuleData();
        ruleData.setPluginName(pluginName);
        ruleData.setSelectorId(list.get(0));
        ruleData.setId(list.get(1));
        Optional.ofNullable(pluginDataSubscriber).ifPresent(e -> e.unRuleSubscribe(ruleData));
        etcdClient.watchClose(dataPath);
    }

    private void cacheAuthData(final String dataString) {
        final AppAuthData appAuthData = GsonUtils.getInstance().fromJson(dataString, AppAuthData.class);
        Optional.ofNullable(appAuthData).ifPresent(data -> authDataSubscribers.forEach(e -> e.onSubscribe(data)));
    }

    private void unCacheAuthData(final String dataPath) {
        final String key = dataPath.substring(DefaultPathConstants.APP_AUTH_PARENT.length() + 1);
        AppAuthData appAuthData = new AppAuthData();
        appAuthData.setAppKey(key);
        authDataSubscribers.forEach(e -> e.unSubscribe(appAuthData));
        etcdClient.watchClose(dataPath);
    }

    private void cacheMetaData(final String dataString) {
        final MetaData metaData = GsonUtils.getInstance().fromJson(dataString, MetaData.class);
        Optional.ofNullable(metaData).ifPresent(data -> metaDataSubscribers.forEach(e -> e.onSubscribe(metaData)));
    }

    private void unCacheMetaData(final MetaData metaData) {
        Optional.ofNullable(metaData).ifPresent(data -> metaDataSubscribers.forEach(e -> e.unSubscribe(metaData)));
    }

    private String buildRealPath(final String parent, final String children) {
        return String.join("/", parent, children);
    }

    private List<String> etcdClientGetChildren(final String parent) {
        return etcdClient.getChildrenKeys(parent, "/");
    }

    @Override
    public void close() {
        if (null != etcdClient) {
            etcdClient.close();
        }
    }
}
