package io.moquette.delaygrouping.monitoring;

import org.junit.Test;

import java.util.concurrent.Executors;

public class ConnectionMonitorTest {

    @Test
    public void testPongEndpoint() {
        new ConnectionMonitor(1885, Executors.newScheduledThreadPool(1));
        while (true) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
