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

package org.apache.shenyu.admin.listener.websocket;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.shenyu.admin.service.SyncDataService;
import org.apache.shenyu.admin.spring.SpringBeanUtils;
import org.apache.shenyu.admin.utils.ThreadLocalUtil;
import org.apache.shenyu.common.enums.DataEventTypeEnum;

import javax.websocket.OnClose;
import javax.websocket.OnError;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.ServerEndpoint;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * The type Websocket data changed listener.
 *
 * @since 2.0.0
 */
@Slf4j
@ServerEndpoint(value = "/websocket", configurator = WebsocketConfigurator.class)
public class WebsocketCollector {

    private static final Set<Session> SESSION_SET = new CopyOnWriteArraySet<>();

    private static final String SESSION_KEY = "sessionKey";

    /**
     * On open.
     * 成功建立连接后执行
     * @param session the session
     */
    @OnOpen
    public void onOpen(final Session session) {
        log.info("websocket on client[{}] open successful....", getClientIp(session));
        // 保存所有session（比如部署了多个网关）
        SESSION_SET.add(session);
    }

    private static String getClientIp(final Session session) {
        Map<String, Object> userProperties = session.getUserProperties();
        if (MapUtils.isEmpty(userProperties)) {
            return StringUtils.EMPTY;
        }
        Object ipObject = userProperties.get(WebsocketListener.CLIENT_IP_NAME);
        if (null == ipObject) {
            return StringUtils.EMPTY;
        }
        return ipObject.toString();
    }

    /**
     * On message.
     *
     * @param message the message
     * @param session the session
     */
    @OnMessage
    public void onMessage(final String message, final Session session) {
        if (message.equals(DataEventTypeEnum.MYSELF.name())) {
            try {
                // 通过threadlocal维护session
                ThreadLocalUtil.put(SESSION_KEY, session);
                // 同步全部数据
                SpringBeanUtils.getInstance().getBean(SyncDataService.class).syncAll(DataEventTypeEnum.MYSELF);
            } finally {
                // 及时清理 threadlocal
                ThreadLocalUtil.clear();
            }
        }
    }

    /**
     * On close.
     * 关闭时执行
     * @param session the session
     */
    @OnClose
    public void onClose(final Session session) {
        SESSION_SET.remove(session);
        ThreadLocalUtil.clear();
        log.warn("websocket close on client[{}]", getClientIp(session));
    }

    /**
     * On error.
     * 发生错误时执行
     * @param session the session
     * @param error   the error
     */
    @OnError
    public void onError(final Session session, final Throwable error) {
        SESSION_SET.remove(session);
        ThreadLocalUtil.clear();
        log.error("websocket collection on client[{}] error: ", getClientIp(session), error);
    }

    /**
     * Send.
     *
     * @param message the message
     * @param type    the type
     */
    public static void send(final String message, final DataEventTypeEnum type) {
        if (StringUtils.isNotBlank(message)) {
            // 如果是MYSELF（第一次的全量同步）
            if (DataEventTypeEnum.MYSELF == type) {
                // 从threadlocal中获取session
                Session session = (Session) ThreadLocalUtil.get(SESSION_KEY);
                if (session != null) {
                    // 向该session发送全量数据
                    sendMessageBySession(session, message);
                }
            } else {
                // 后续的增量同步
                // 向所有的session中同步变更数据
                SESSION_SET.forEach(session -> sendMessageBySession(session, message));
            }
        }
    }

    private static void sendMessageBySession(final Session session, final String message) {
        try {
            // 通过websocket的session把消息发送出去
            session.getBasicRemote().sendText(message);
        } catch (IOException e) {
            log.error("websocket send result is exception: ", e);
        }
    }
}
