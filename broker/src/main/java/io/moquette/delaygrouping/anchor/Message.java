package io.moquette.delaygrouping.anchor;

import java.nio.charset.StandardCharsets;

public class Message {
    public String topic;
    public byte[] payload;

    public Message(String topic, byte[] payload) {
        this.topic = topic;
        this.payload = payload;
    }

    @Override
    public String toString() {
        return "Message{" +
            "topic='" + topic + '\'' +
            ", payload=" + new String(payload, StandardCharsets.UTF_8) +
            '}';
    }
}
