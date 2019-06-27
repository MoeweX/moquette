package io.moquette.delaygrouping.peering.messaging;

import io.netty.handler.codec.mqtt.MqttPublishMessage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class PeerMessagePublish extends PeerMessage {

    // TODO Encapsulate access to members (or refactor into subtype holding byte payload only)
    List<byte[]> payload = null;
    transient List<MqttPublishMessage> publishMessages = new ArrayList<>();

    public PeerMessagePublish(Collection<MqttPublishMessage> messages) {
        super(PeerMessageType.PUBLISH);
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
