package io.moquette.integration;

import io.moquette.delaygrouping.monitoring.ConnectionMonitor;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

public class ConnectionMonitorTest {

    @Test
    public void testPingPong() {
        ConnectionMonitor monitor1 = new ConnectionMonitor(1885, Executors.newScheduledThreadPool(1));
        ConnectionMonitor monitor2 = new ConnectionMonitor(2885, Executors.newScheduledThreadPool(1));

        monitor1.addMonitoredPeer(new InetSocketAddress("localhost", 2885));
        monitor2.addMonitoredPeer(new InetSocketAddress("localhost", 1885));

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println(monitor1.getStats(new InetSocketAddress("localhost", 2885)));
        System.out.println(monitor2.getStats(new InetSocketAddress("localhost", 1885)));
    }
}

