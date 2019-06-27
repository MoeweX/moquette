package io.moquette.delaygrouping.mqtt;

public class Message {
    public String topic;
    public byte[] payload;

    public Message(String topic, byte[] payload) {
        this.topic = topic;
        this.payload = payload;
    }
}
