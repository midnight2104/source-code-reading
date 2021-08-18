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

import io.etcd.jetcd.Client;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.options.DeleteOption;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.WatchOption;
import io.etcd.jetcd.watch.WatchEvent;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Etcd client of Bootstrap.
 * 网关端的etcd客户端
 * client.getKVClient().get(key).get().getKvs(); 获取 KeyValue 对象
 * client.getWatchClient().watch(key, listener); 为 key 添加监听事件，实现数据订阅
 */
@Slf4j
public class EtcdClient {

    private final Client client;

    private final ConcurrentHashMap<String, Watch.Watcher> watchCache = new ConcurrentHashMap<>();

    public EtcdClient(final Client client) {
        this.client = client;
    }

    /**
     * close client.
     */
    public void close() {
        this.client.close();
    }

    /**
     * get node value.
     * @param key node name
     * @return string
     */
    @SneakyThrows
    public String get(final String key) {
        List<KeyValue> keyValues = client.getKVClient().get(ByteSequence.from(key, StandardCharsets.UTF_8)).get().getKvs();
        return keyValues.isEmpty() ? null : keyValues.iterator().next().getValue().toString(StandardCharsets.UTF_8);
    }

    /**
     * get node sub nodes.
     * 获取节点的子节点
     * @param prefix node prefix.
     * @param separator separator char
     * @return sub nodes
     */
    @SneakyThrows
    public List<String> getChildrenKeys(final String prefix, final String separator) {
        ByteSequence prefixByteSequence = ByteSequence.from(prefix, StandardCharsets.UTF_8);
        GetOption getOption = GetOption.newBuilder().withPrefix(prefixByteSequence).withSortField(GetOption.SortTarget.KEY).withSortOrder(GetOption.SortOrder.ASCEND).build();
        List<KeyValue> keyValues = client.getKVClient().get(prefixByteSequence, getOption).get().getKvs();
        return keyValues.stream().map(e -> getSubNodeKeyName(prefix, e.getKey().toString(StandardCharsets.UTF_8), separator)).distinct().collect(Collectors.toList());
    }

    private String getSubNodeKeyName(final String prefix, final String fullPath, final String separator) {
        String pathWithoutPrefix = fullPath.substring(prefix.length());
        return pathWithoutPrefix.contains(separator) ? pathWithoutPrefix.substring(1) : pathWithoutPrefix;
    }

    /**
     * update value of node.
     * @param key node name
     * @param value node value
     */
    @SneakyThrows
    public void put(final String key, final String value) {
        client.getKVClient().put(ByteSequence.from(key, StandardCharsets.UTF_8), ByteSequence.from(value, StandardCharsets.UTF_8)).get();
    }

    /**
     * delete node.
     * @param key node name
     */
    public void delete(final String key) {
        client.getKVClient().delete(ByteSequence.from(key, StandardCharsets.UTF_8));
    }

    /**
     * delete node of recursive.
     * @param key parent node name
     */
    public void deleteRecursive(final String key) {
        DeleteOption option = DeleteOption.newBuilder()
                .withPrefix(ByteSequence.from(key, StandardCharsets.UTF_8))
                .build();
        client.getKVClient().delete(ByteSequence.from(key, StandardCharsets.UTF_8), option);
    }

    /**
     * subscribe data change.
     * 订阅数据变更
     * @param key node name
     * @param updateHandler node value handler of update
     * @param deleteHandler node value handler of delete
     */
    public void watchDataChange(final String key, final BiConsumer<String, String> updateHandler, final Consumer<String> deleteHandler) {
        // 得到监听事件
        Watch.Listener listener = watch(updateHandler, deleteHandler);
        // 为key添加监听事件
        Watch.Watcher watch = client.getWatchClient().watch(ByteSequence.from(key, StandardCharsets.UTF_8), listener);
        watchCache.put(key, watch);
    }

    /**
     * subscribe sub node change.
     * 订阅子节点变更
     * @param key param node name.
     * @param updateHandler sub node handler of update
     * @param deleteHandler sub node delete of delete
     */
    public void watchChildChange(final String key, final BiConsumer<String, String> updateHandler, final Consumer<String> deleteHandler) {
        // 得到监听事件
        Watch.Listener listener = watch(updateHandler, deleteHandler);
        WatchOption option = WatchOption.newBuilder()
                .withPrefix(ByteSequence.from(key, StandardCharsets.UTF_8))
                .build();
        // 为key添加监听事件，节点变更
        Watch.Watcher watch = client.getWatchClient().watch(ByteSequence.from(key, StandardCharsets.UTF_8), option, listener);
        watchCache.put(key, watch);
    }

    private Watch.Listener watch(final BiConsumer<String, String> updateHandler, final Consumer<String> deleteHandler) {
        // 实例化一个监听事件，当key变化时会被调用
        return Watch.listener(response -> {
            for (WatchEvent event: response.getEvents()) {
                String path = event.getKeyValue().getKey().toString(StandardCharsets.UTF_8);
                String value = event.getKeyValue().getValue().toString(StandardCharsets.UTF_8);
                switch (event.getEventType()) {
                    case PUT:
                        updateHandler.accept(path, value); //执行更新操作
                        continue;
                    case DELETE:
                        deleteHandler.accept(path); //执行删除操作
                        continue;
                    default:
                }
            }
        });
    }

    /**
     * cancel subscribe.
     * 取消订阅
     * @param key node name
     */
    public void watchClose(final String key) {
        if (watchCache.containsKey(key)) {
            watchCache.get(key).close();
            watchCache.remove(key);
        }
    }
}
