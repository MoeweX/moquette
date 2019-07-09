package io.moquette.logging;

import io.netty.handler.codec.mqtt.MqttPublishMessage;

import java.nio.charset.StandardCharsets;

public class NettyMqttPubLogMessage extends LogMessage {
    private MqttPublishMessage msg;

    public NettyMqttPubLogMessage(MqttPublishMessage msg) {
        this.msg = msg;
        msg.retain();
    }

    @Override
    public String toLogEntry() {
        var payloadSize = msg.payload().readableBytes();
        var topic = msg.variableHeader().topicName();
        byte[] buf;
        if (payloadSize > 200) {
            buf = new byte[200];
            msg.payload().slice().getBytes(0, buf);
        } else {
            buf = new byte[payloadSize];
            msg.payload().slice().getBytes(0, buf);
        }
        msg.release();

        var fields = new String(buf, StandardCharsets.US_ASCII).split(";");

        if (fields.length < 2) {
            return "unknown_id;" + topic + ";" + "unknown_sent_time;" + receivedTimestamp + ";" + payloadSize;
        } else {
            return fields[0] + ";" + topic + ";" + fields[1] + ";" + receivedTimestamp + ";" + payloadSize;
        }
    }
}
