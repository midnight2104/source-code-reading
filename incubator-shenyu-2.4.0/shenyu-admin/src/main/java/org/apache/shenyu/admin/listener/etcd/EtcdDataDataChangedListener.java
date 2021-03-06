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

package org.apache.shenyu.admin.listener.etcd;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.shenyu.admin.listener.DataChangedListener;
import org.apache.shenyu.common.constant.DefaultPathConstants;
import org.apache.shenyu.common.dto.AppAuthData;
import org.apache.shenyu.common.dto.MetaData;
import org.apache.shenyu.common.dto.PluginData;
import org.apache.shenyu.common.dto.SelectorData;
import org.apache.shenyu.common.dto.RuleData;
import org.apache.shenyu.common.enums.DataEventTypeEnum;
import org.apache.shenyu.common.utils.GsonUtils;

import java.net.URLEncoder;
import java.util.List;

/**
 * EtcdDataDataChangedListener.
 * Etcd数据变更监听器
 */
@Slf4j
public class EtcdDataDataChangedListener implements DataChangedListener {

    private final EtcdClient etcdClient;

    public EtcdDataDataChangedListener(final EtcdClient client) {
        this.etcdClient = client;
    }

    /**
     * 处理认证信息
     * @param changed   the changed
     * @param eventType the event type
     */
    @Override
    public void onAppAuthChanged(final List<AppAuthData> changed, final DataEventTypeEnum eventType) {
        for (AppAuthData data : changed) {
            String appAuthPath = DefaultPathConstants.buildAppAuthPath(data.getAppKey());
            // delete
            if (eventType == DataEventTypeEnum.DELETE) {
                etcdClient.delete(appAuthPath);
                continue;
            }
            // create or update
            updateNode(appAuthPath, data);
        }
    }

    /**
     * 处理插件
     * @param changed   the changed
     * @param eventType the event type
     */
    @Override
    public void onPluginChanged(final List<PluginData> changed, final DataEventTypeEnum eventType) {
        for (PluginData data : changed) {
            String pluginPath = DefaultPathConstants.buildPluginPath(data.getName());
            // delete
            if (eventType == DataEventTypeEnum.DELETE) {
                etcdClient.deleteEtcdPathRecursive(pluginPath);
                String selectorParentPath = DefaultPathConstants.buildSelectorParentPath(data.getName());
                etcdClient.deleteEtcdPathRecursive(selectorParentPath);
                String ruleParentPath = DefaultPathConstants.buildRuleParentPath(data.getName());
                etcdClient.deleteEtcdPathRecursive(ruleParentPath);
                continue;
            }
            //create or update
            updateNode(pluginPath, data);
        }
    }

    /**
     * 处理选择器
     * @param changed   the changed
     * @param eventType the event type
     */
    @Override
    public void onSelectorChanged(final List<SelectorData> changed, final DataEventTypeEnum eventType) {
        if (eventType == DataEventTypeEnum.REFRESH && !changed.isEmpty()) {
            String selectorParentPath = DefaultPathConstants.buildSelectorParentPath(changed.get(0).getPluginName());
            etcdClient.deleteEtcdPathRecursive(selectorParentPath);
        }
        for (SelectorData data : changed) {
            String selectorRealPath = DefaultPathConstants.buildSelectorRealPath(data.getPluginName(), data.getId());
            if (eventType == DataEventTypeEnum.DELETE) {
                etcdClient.delete(selectorRealPath);
                continue;
            }
            //create or update
            updateNode(selectorRealPath, data);
        }
    }

    /**
     * 处理元数据
     * @param changed   the changed
     * @param eventType the event type
     */
    @SneakyThrows
    @Override
    public void onMetaDataChanged(final List<MetaData> changed, final DataEventTypeEnum eventType) {
        for (MetaData data : changed) {
            String metaDataPath = DefaultPathConstants.buildMetaDataPath(URLEncoder.encode(data.getPath(), "UTF-8"));
            // delete
            if (eventType == DataEventTypeEnum.DELETE) {
                etcdClient.delete(metaDataPath);
                continue;
            }
            // create or update
            updateNode(metaDataPath, data);
        }
    }

    /**
     * 处理规则
     * @param changed   the changed
     * @param eventType the event type
     */
    @Override
    public void onRuleChanged(final List<RuleData> changed, final DataEventTypeEnum eventType) {
        if (eventType == DataEventTypeEnum.REFRESH && !changed.isEmpty()) {
            String selectorParentPath = DefaultPathConstants.buildRuleParentPath(changed.get(0).getPluginName());
            etcdClient.deleteEtcdPathRecursive(selectorParentPath);
        }
        for (RuleData data : changed) {
            String ruleRealPath = DefaultPathConstants.buildRulePath(data.getPluginName(), data.getSelectorId(), data.getId());
            if (eventType == DataEventTypeEnum.DELETE) {
                etcdClient.delete(ruleRealPath);
                continue;
            }
            //create or update
            updateNode(ruleRealPath, data);
        }
    }

    /**
     * 将数据发送到etcd
     * @param pluginPath
     * @param data
     */
    private void updateNode(final String pluginPath, final Object data) {
        etcdClient.put(pluginPath, GsonUtils.getInstance().toJson(data));
    }
}
