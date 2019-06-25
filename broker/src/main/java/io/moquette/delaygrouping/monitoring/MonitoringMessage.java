package io.moquette.delaygrouping.monitoring;

import java.net.InetSocketAddress;

public class MonitoringMessage {
    String message;
    InetSocketAddress sender;

    public MonitoringMessage(String message, InetSocketAddress sender) {
        this.message = message;
        this.sender = sender;
    }

    @Override
    public String toString() {
        return "MonitoringMessage(message=\"" + message + "\", sender=" + sender + ")";
    }
}
