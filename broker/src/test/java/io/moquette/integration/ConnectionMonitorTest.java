package io.moquette.integration;

import io.moquette.delaygrouping.monitoring.ConnectionMonitor;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.concurrent.Executors;

public class ConnectionMonitorTest {

    @Test
    public void testPingPong() {
        ConnectionMonitor monitor1 = new ConnectionMonitor(1885, Executors.newScheduledThreadPool(4));
        ConnectionMonitor monitor2 = new ConnectionMonitor(2885, Executors.newScheduledThreadPool(4));

        monitor1.addMonitoredPeer(new InetSocketAddress("localhost", 2885));
        monitor2.addMonitoredPeer(new InetSocketAddress("localhost", 1885));

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        DescriptiveStatistics stats1 = monitor1.getStats(new InetSocketAddress("localhost", 2885));
        DescriptiveStatistics stats2 = monitor2.getStats(new InetSocketAddress("localhost", 1885));
        System.out.println(stats1);
        System.out.println(Arrays.toString(stats1.getValues()));
        System.out.println(stats2);
        System.out.println(Arrays.toString(stats2.getValues()));
    }
}

