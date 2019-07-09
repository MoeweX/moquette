package io.moquette.logging;

import java.time.Instant;

abstract class LogMessage {
    long receivedTimestamp;

    private static long getEpochMicro() {
        Instant now = Instant.now();
        return (now.getEpochSecond() * 1000000) + (now.getNano() / 1000);
    }

    public abstract String toLogEntry();

    public void timestamp() {
        receivedTimestamp = getEpochMicro();
    }
}
