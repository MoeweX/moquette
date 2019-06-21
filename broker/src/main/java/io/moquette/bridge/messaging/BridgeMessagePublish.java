package io.moquette.bridge.messaging;

import io.netty.handler.codec.mqtt.MqttPublishMessage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class BridgeMessagePublish extends BridgeMessage {

    // TODO Encapsulate access to members (or refactor into subtype holding byte payload only)
    List<byte[]> payload = null;
    transient List<MqttPublishMessage> publishMessages = new ArrayList<>();

    public BridgeMessagePublish(Collection<MqttPublishMessage> messages) {
        super(BridgeMessageType.PUBLISH);
        this.publishMessages.addAll(messages);
    }

    @Override
    public void retain() {
        publishMessages.forEach(MqttPublishMessage::retain);
    }

    public List<MqttPublishMessage> getPublishMessages() {
        return publishMessages;
    }

}
