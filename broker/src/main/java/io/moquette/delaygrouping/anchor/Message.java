package io.moquette.delaygrouping.anchor;

public class Message {
    public String topic;
    public byte[] payload;

    public Message(String topic, byte[] payload) {
        this.topic = topic;
        this.payload = payload;
    }
}
