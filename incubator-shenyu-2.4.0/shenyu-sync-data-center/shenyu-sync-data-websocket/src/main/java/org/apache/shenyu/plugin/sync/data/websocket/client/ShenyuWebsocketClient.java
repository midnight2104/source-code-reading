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

package org.apache.shenyu.plugin.sync.data.websocket.client;

import java.net.URI;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.shenyu.common.dto.WebsocketData;
import org.apache.shenyu.common.enums.ConfigGroupEnum;
import org.apache.shenyu.common.enums.DataEventTypeEnum;
import org.apache.shenyu.common.utils.GsonUtils;
import org.apache.shenyu.plugin.sync.data.websocket.handler.WebsocketDataHandler;
import org.apache.shenyu.sync.data.api.AuthDataSubscriber;
import org.apache.shenyu.sync.data.api.MetaDataSubscriber;
import org.apache.shenyu.sync.data.api.PluginDataSubscriber;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;

/**
 * WebSocketClient
 * The type shenyu websocket client.
 */
@Slf4j
public final class ShenyuWebsocketClient extends WebSocketClient {
    
    private volatile boolean alreadySync = Boolean.FALSE;
    
    private final WebsocketDataHandler websocketDataHandler;
    
    /**
     * Instantiates a new shenyu websocket client.
     *
     * @param serverUri             the server uri
     * @param pluginDataSubscriber the plugin data subscriber
     * @param metaDataSubscribers   the meta data subscribers
     * @param authDataSubscribers   the auth data subscribers
     */
    public ShenyuWebsocketClient(final URI serverUri, final PluginDataSubscriber pluginDataSubscriber,
                                 final List<MetaDataSubscriber> metaDataSubscribers, final List<AuthDataSubscriber> authDataSubscribers) {
        super(serverUri);
        this.websocketDataHandler = new WebsocketDataHandler(pluginDataSubscriber, metaDataSubscribers, authDataSubscribers);
    }

    // 成功建立连接后执行
    @Override
    public void onOpen(final ServerHandshake serverHandshake) {
        // 防止重新建立连接时，再次执行，所以用alreadySync进行判断
        if (!alreadySync) {
            // 同步所有数据，MYSELF 类型
            send(DataEventTypeEnum.MYSELF.name());
            alreadySync = true;
        }
    }

    // 接受到消息后执行
    @Override
    public void onMessage(final String result) {
        // 处理接收到的数据
        handleResult(result);
    }
    
    @Override
    public void onClose(final int i, final String s, final boolean b) {
        this.close();
    }
    
    @Override
    public void onError(final Exception e) {
        this.close();
    }
    
    @SuppressWarnings("ALL")
    private void handleResult(final String result) {
        // 数据反序列化
        WebsocketData websocketData = GsonUtils.getInstance().fromJson(result, WebsocketData.class);
        // 哪种数据类型，插件、选择器、规则...
        ConfigGroupEnum groupEnum = ConfigGroupEnum.acquireByName(websocketData.getGroupType());
        // 哪种操作类型，更新、删除...
        String eventType = websocketData.getEventType();
        String json = GsonUtils.getInstance().toJson(websocketData.getData());

        // 处理数据
        websocketDataHandler.executor(groupEnum, json, eventType);
    }
}
