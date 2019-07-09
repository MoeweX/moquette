package io.moquette.integration;

import io.moquette.delaygrouping.monitoring.ConnectionMonitor;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.junit.Ignore;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

public class ConnectionMonitorTest {

    @Test
    @Ignore
    public void testManyPings() throws ExecutionException, InterruptedException, UnknownHostException {
        ConnectionMonitor monitor1 = new ConnectionMonitor(1000, 20);

        for (int i = 1; i < 250; i++) {
            monitor1.addMonitoredPeer(InetAddress.getByName("127.0.0." + i));
        }

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        DescriptiveStatistics stats1 = monitor1.getStats(InetAddress.getByName("127.0.0.1")).get();
        System.out.println(stats1.getMean());
        System.out.println(Arrays.toString(stats1.getValues()));
    }

    @Test
    public void testPing() throws ExecutionException, InterruptedException, UnknownHostException {
        ConnectionMonitor monitor1 = new ConnectionMonitor(1000, 20);

        monitor1.addMonitoredPeer(InetAddress.getByName("127.0.0.1"));

        try {
            Thread.sleep(4000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        DescriptiveStatistics stats1 = monitor1.getStats(InetAddress.getByName("127.0.0.1")).get();
        assertThat(stats1).isNotNull();
        assertThat(stats1.getValues()).isNotEmpty();
    }

    @Test
    public void testPingAddAndRemove() throws ExecutionException, InterruptedException, UnknownHostException {
        ConnectionMonitor monitor1 = new ConnectionMonitor(1000, 20);

        monitor1.addMonitoredPeer(InetAddress.getByName("127.0.0.1"));
        monitor1.addMonitoredPeer(InetAddress.getByName("127.0.0.2"));

        try {
            Thread.sleep(4000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        DescriptiveStatistics stats1 = monitor1.getStats(InetAddress.getByName("127.0.0.1")).get();
        assertThat(stats1).isNotNull();
        assertThat(stats1.getValues()).isNotEmpty();

        DescriptiveStatistics stats2 = monitor1.getStats(InetAddress.getByName("127.0.0.2")).get();
        assertThat(stats2).isNotNull();
        assertThat(stats2.getValues()).isNotEmpty();

        monitor1.removeMonitoredPeer(InetAddress.getByName("127.0.0.1"));

        stats1 = monitor1.getStats(InetAddress.getByName("127.0.0.1")).get();
        assertThat(stats1).isNull();

        stats2 = monitor1.getStats(InetAddress.getByName("127.0.0.2")).get();
        assertThat(stats2).isNotNull();
        assertThat(stats2.getValues()).isNotEmpty();
    }

    @Test
    public void chaosTest() throws UnknownHostException, InterruptedException, ExecutionException {
        var monitor1 = new ConnectionMonitor(1000, 20);

        var addr1 = InetAddress.getByName("127.0.0.1");
        var addr2 = InetAddress.getByName("127.0.0.2");
        var addr3 = InetAddress.getByName("127.0.0.3");
        var addr4 = InetAddress.getByName("127.0.0.4");
        var addr5 = InetAddress.getByName("127.0.0.5");

        monitor1.addMonitoredPeer(addr1);
        monitor1.addMonitoredPeer(addr2);
        monitor1.addMonitoredPeer(addr3);
        monitor1.addMonitoredPeer(addr4);
        monitor1.addMonitoredPeer(addr5);

        for (int i = 1; i < 100; i++) {
            assertThat(monitor1.getAverageDelay(addr1).get()).isNotNaN().isNotNull();
            assertThat(monitor1.getAverageDelay(addr2).get()).isNotNaN().isNotNull();
            assertThat(monitor1.getAverageDelay(addr3).get()).isNotNaN().isNotNull();
            assertThat(monitor1.getAverageDelay(addr4).get()).isNotNaN().isNotNull();
            assertThat(monitor1.getAverageDelay(addr5).get()).isNotNaN().isNotNull();
            Thread.sleep(100);
        }

        monitor1.removeAll();
        monitor1.addMonitoredPeer(addr1);

        for (int i = 1; i < 50; i++) {
            assertThat(monitor1.getAverageDelay(addr1).get()).isNotNaN().isNotNull();
            Thread.sleep(100);
        }

    }
}

