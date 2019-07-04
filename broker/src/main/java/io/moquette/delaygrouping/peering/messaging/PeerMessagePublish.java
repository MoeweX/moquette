package io.moquette.delaygrouping.peering.messaging;

import io.netty.handler.codec.mqtt.MqttPublishMessage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class PeerMessagePublish extends PeerMessage {

    // TODO Encapsulate access to members (or refactor into subtype holding byte payload only)
    List<byte[]> payload = null;
    private transient List<MqttPublishMessage> publishMessages = new ArrayList<>();

    private PeerMessagePublish() {
        super(PeerMessageType.PUBLISH);
    }

    public static PeerMessagePublish fromMessage(MqttPublishMessage pubMsg) {
        var msg = new PeerMessagePublish();
        msg.publishMessages.add(pubMsg);
        return msg;
    }

    public static PeerMessagePublish fromMessageList(Collection<MqttPublishMessage> messages) {
        var msg = new PeerMessagePublish();
        msg.publishMessages.addAll(messages);
        return msg;
    }

    @Override
    public void retain() {
        publishMessages.forEach(MqttPublishMessage::retain);
    }

    public List<MqttPublishMessage> getPublishMessages() {
        return publishMessages;
    }

}
