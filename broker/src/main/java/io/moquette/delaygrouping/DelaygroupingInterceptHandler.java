package io.moquette.delaygrouping;

import io.moquette.bridge.BridgeConnection;
import io.moquette.interception.AbstractInterceptHandler;
import io.moquette.interception.messages.InterceptPublishMessage;
import io.moquette.interception.messages.InterceptSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

public class DelaygroupingInterceptHandler extends AbstractInterceptHandler {

    private static final Logger LOG = LoggerFactory.getLogger(BridgeConnection.class);

    private final Consumer<MqttPublishMessage> publishHandler;
    private final Consumer<String> subscribeHandler;

    public DelaygroupingInterceptHandler(Consumer<MqttPublishMessage> publishHandler, Consumer<String> subscribeHandler) {
        this.publishHandler = publishHandler;
        this.subscribeHandler = subscribeHandler;
    }

    @Override
    public Class<?>[] getInterceptedMessageTypes() {
        return new Class[]{
            InterceptPublishMessage.class,
            InterceptSubscribeMessage.class};
    }

    @Override
    public void onSubscribe(InterceptSubscribeMessage msg) {
        subscribeHandler.accept(msg.getTopicFilter());
    }

    @Override
    public void onPublish(InterceptPublishMessage msg) {
        publishHandler.accept(msg.getMsg());
    }

    @Override
    public String getID() {
        return "DelaygroupingInterceptHandler";
    }
}
