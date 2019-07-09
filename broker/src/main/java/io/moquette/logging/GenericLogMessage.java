package io.moquette.logging;

import java.nio.charset.StandardCharsets;

public class GenericLogMessage extends LogMessage {
    private String topic;
    private byte[] payload;

    public GenericLogMessage(String topic, byte[] payload) {
        this.topic = topic;
        this.payload = payload;
    }

    @Override
    public String toLogEntry() {
        byte[] buf;
        if (payload.length > 200) {
            buf = new byte[200];
            System.arraycopy(payload, 0, buf, 0, 500);
        } else {
            buf = payload;
        }
        var fields = new String(buf, StandardCharsets.US_ASCII).split(";");

        if (fields.length < 2) {
            return "unknown_id;" + topic + ";" + "unknown_sent_time;" + receivedTimestamp + ";" + payload.length;
        } else {
            return fields[0] + ";" + topic + ";" + fields[1] + ";" + receivedTimestamp + ";" + payload.length;
        }
    }
}
