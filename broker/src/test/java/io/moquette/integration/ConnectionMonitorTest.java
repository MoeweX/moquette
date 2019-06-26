package io.moquette.integration;

import io.moquette.delaygrouping.monitoring.ConnectionMonitor;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

public class ConnectionMonitorTest {

    @Test
    public void testPingPong() throws ExecutionException, InterruptedException {
        ConnectionMonitor monitor1 = new ConnectionMonitor(1885, Executors.newScheduledThreadPool(1), 1000, 20);
        ConnectionMonitor monitor2 = new ConnectionMonitor(2885, Executors.newScheduledThreadPool(1), 1000, 20);

        monitor1.addMonitoredPeer(new InetSocketAddress("localhost", 2885));
        monitor2.addMonitoredPeer(new InetSocketAddress("localhost", 1885));

        while (true) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            DescriptiveStatistics stats1 = monitor1.getStats(new InetSocketAddress("localhost", 2885)).get();
            DescriptiveStatistics stats2 = monitor2.getStats(new InetSocketAddress("localhost", 1885)).get();
            System.out.println(stats1.getMean());
            System.out.println(Arrays.toString(stats1.getValues()));
            System.out.println(stats2.getMean());
            System.out.println(Arrays.toString(stats2.getValues()));

        }

    }
}

