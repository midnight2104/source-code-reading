package org.apache.shenyu.examples.http.websocket;

import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient;
import org.springframework.web.reactive.socket.client.WebSocketClient;
import reactor.core.publisher.Flux;

import java.net.URI;
import java.time.Duration;

public class WSClient {
    public static void main(final String[] args) {
        String url ="ws://localhost:9195/?module=ws&method=/echo&rpcType=websocket";
       // String url = "ws://localhost:8189/echo";
        final WebSocketClient client = new ReactorNettyWebSocketClient();
        client.execute(URI.create(url), session ->
                session.send(Flux.just(session.textMessage("你好lll")))
                        .thenMany(session.receive().take(1).map(WebSocketMessage::getPayloadAsText))
                        .doOnNext(System.out::println)
                        .then())
                .block(Duration.ofMillis(50000));


    }
}