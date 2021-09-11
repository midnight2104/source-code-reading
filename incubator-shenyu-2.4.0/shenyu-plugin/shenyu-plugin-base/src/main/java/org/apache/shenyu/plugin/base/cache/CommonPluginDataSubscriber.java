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

package org.apache.shenyu.plugin.base.cache;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.shenyu.common.dto.PluginData;
import org.apache.shenyu.common.dto.RuleData;
import org.apache.shenyu.common.dto.SelectorData;
import org.apache.shenyu.common.enums.DataEventTypeEnum;
import org.apache.shenyu.plugin.base.handler.PluginDataHandler;
import org.apache.shenyu.sync.data.api.PluginDataSubscriber;

/**
 * 通用插件数据订阅者，负责处理所有插件、选择器和规则信息
 * The type Common plugin data subscriber.
 */
public class CommonPluginDataSubscriber implements PluginDataSubscriber {

    private final Map<String, PluginDataHandler> handlerMap;

    /**
     * Instantiates a new Common plugin data subscriber.
     *
     * @param pluginDataHandlerList the plugin data handler list
     */
    public CommonPluginDataSubscriber(final List<PluginDataHandler> pluginDataHandlerList) {
        // 每个插件都有自己的 数据处理器
        this.handlerMap = pluginDataHandlerList.stream().collect(Collectors.toConcurrentMap(PluginDataHandler::pluginNamed, e -> e));
    }

    // 处理插件数据
    @Override
    public void onSubscribe(final PluginData pluginData) {
        subscribeDataHandler(pluginData, DataEventTypeEnum.UPDATE);
    }

    @Override
    public void unSubscribe(final PluginData pluginData) {
        subscribeDataHandler(pluginData, DataEventTypeEnum.DELETE);
    }

    @Override
    public void refreshPluginDataAll() {
        BaseDataCache.getInstance().cleanPluginData();
    }

    @Override
    public void refreshPluginDataSelf(final List<PluginData> pluginDataList) {
        if (CollectionUtils.isEmpty(pluginDataList)) {
            return;
        }
        BaseDataCache.getInstance().cleanPluginDataSelf(pluginDataList);
    }

    // 处理选择器数据
    @Override
    public void onSelectorSubscribe(final SelectorData selectorData) {
        subscribeDataHandler(selectorData, DataEventTypeEnum.UPDATE);
    }

    @Override
    public void unSelectorSubscribe(final SelectorData selectorData) {
        subscribeDataHandler(selectorData, DataEventTypeEnum.DELETE);
    }

    @Override
    public void refreshSelectorDataAll() {
        BaseDataCache.getInstance().cleanSelectorData();
    }

    @Override
    public void refreshSelectorDataSelf(final List<SelectorData> selectorDataList) {
        if (CollectionUtils.isEmpty(selectorDataList)) {
            return;
        }
        BaseDataCache.getInstance().cleanSelectorDataSelf(selectorDataList);
    }

    // 处理规则数据
    @Override
    public void onRuleSubscribe(final RuleData ruleData) {
        subscribeDataHandler(ruleData, DataEventTypeEnum.UPDATE);
    }

    @Override
    public void unRuleSubscribe(final RuleData ruleData) {
        subscribeDataHandler(ruleData, DataEventTypeEnum.DELETE);
    }

    @Override
    public void refreshRuleDataAll() {
        BaseDataCache.getInstance().cleanRuleData();
    }

    @Override
    public void refreshRuleDataSelf(final List<RuleData> ruleDataList) {
        if (CollectionUtils.isEmpty(ruleDataList)) {
            return;
        }
        BaseDataCache.getInstance().cleanRuleDataSelf(ruleDataList);
    }

    // 订阅数据处理器，处理数据的更新或删除
    private <T> void subscribeDataHandler(final T classData, final DataEventTypeEnum dataType) {
        Optional.ofNullable(classData).ifPresent(data -> {
            // 插件数据
            if (data instanceof PluginData) {
                PluginData pluginData = (PluginData) data;
                if (dataType == DataEventTypeEnum.UPDATE) { // 更新操作
                    // 将数据保存到网关内存
                    BaseDataCache.getInstance().cachePluginData(pluginData);
                    // 如果每个插件还有自己的处理逻辑，那么就去处理
                    Optional.ofNullable(handlerMap.get(pluginData.getName())).ifPresent(handler -> handler.handlerPlugin(pluginData));
                } else if (dataType == DataEventTypeEnum.DELETE) {  // 删除操作
                    // 从网关内存移除数据
                    BaseDataCache.getInstance().removePluginData(pluginData);
                    // 如果每个插件还有自己的处理逻辑，那么就去处理
                    Optional.ofNullable(handlerMap.get(pluginData.getName())).ifPresent(handler -> handler.removePlugin(pluginData));
                }
            } else if (data instanceof SelectorData) {  // 选择器数据
                SelectorData selectorData = (SelectorData) data;
                if (dataType == DataEventTypeEnum.UPDATE) { // 更新操作
                    // 将数据保存到网关内存
                    BaseDataCache.getInstance().cacheSelectData(selectorData);
                    // 如果每个插件还有自己的处理逻辑，那么就去处理
                    Optional.ofNullable(handlerMap.get(selectorData.getPluginName())).ifPresent(handler -> handler.handlerSelector(selectorData));
                } else if (dataType == DataEventTypeEnum.DELETE) {  // 删除操作
                    // 从网关内存移除数据
                    BaseDataCache.getInstance().removeSelectData(selectorData);
                    // 如果每个插件还有自己的处理逻辑，那么就去处理
                    Optional.ofNullable(handlerMap.get(selectorData.getPluginName())).ifPresent(handler -> handler.removeSelector(selectorData));
                }
            } else if (data instanceof RuleData) {  // 规则数据
                RuleData ruleData = (RuleData) data;
                if (dataType == DataEventTypeEnum.UPDATE) { // 更新操作
                    // 将数据保存到网关内存
                    BaseDataCache.getInstance().cacheRuleData(ruleData);
                    // 如果每个插件还有自己的处理逻辑，那么就去处理
                    Optional.ofNullable(handlerMap.get(ruleData.getPluginName())).ifPresent(handler -> handler.handlerRule(ruleData));
                } else if (dataType == DataEventTypeEnum.DELETE) { // 删除操作
                    // 从网关内存移除数据
                    BaseDataCache.getInstance().removeRuleData(ruleData);
                    // 如果每个插件还有自己的处理逻辑，那么就去处理
                    Optional.ofNullable(handlerMap.get(ruleData.getPluginName())).ifPresent(handler -> handler.removeRule(ruleData));
                }
            }
        });
    }
}
