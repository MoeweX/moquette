package io.moquette.delaygrouping.monitoring;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.concurrent.*;

public class ConnectionMonitor {

    private static final Logger LOG = LoggerFactory.getLogger(ConnectionMonitor.class);
    private final LinkedBlockingQueue<InetSocketAddress> pingRequestQueue;
    private final LinkedBlockingQueue<UDPListener.Measurement> measurementQueue;
    private final int windowSize;
    private ConcurrentHashMap<InetSocketAddress, DescriptiveStatistics> monitoredPeers = new ConcurrentHashMap<>();
    private UDPListener udpListener;
    private ExecutorService resultExecutor = Executors.newSingleThreadExecutor();

    public ConnectionMonitor(int listenPort, ScheduledExecutorService executorService, int interval, int windowSize) {
        this.windowSize = windowSize;

        LOG.info("Starting connection monitor...");

        pingRequestQueue = new LinkedBlockingQueue<>();
        measurementQueue = new LinkedBlockingQueue<>();

        try {
            udpListener = new UDPListener(listenPort, pingRequestQueue, measurementQueue);
            udpListener.start();
            LOG.info("Connection monitor started (ping interval = {}ms). Listening on port {}", interval, listenPort);
        } catch (SocketException e) {
            LOG.error("Error starting listener on port {}: {}", listenPort, e);
        }

        executorService.scheduleAtFixedRate(() -> {
            // Send ping requests
            monitoredPeers.forEach((address, stats) -> {
                pingRequestQueue.add(address);
            });

            // Trigger result collection
            collectResults();
        }, 0, interval, TimeUnit.MILLISECONDS);

    }

    private void collectResults() {
        resultExecutor.execute(() -> {
            ArrayList<UDPListener.Measurement> newResults = new ArrayList<>();
            measurementQueue.drainTo(newResults);

            newResults.forEach(measurement -> {
                monitoredPeers.get(measurement.peerAddress).addValue(measurement.delayValue);
            });
        });
    }

    public void shutdown() {
        udpListener.shutdown();
    }

    public void addMonitoredPeer(InetSocketAddress address) {
        monitoredPeers.put(address, new DescriptiveStatistics(windowSize));
        LOG.info("STARTED monitoring peer at {}", address);
    }

    public void removeMonitoredPeer(InetSocketAddress address) {
        monitoredPeers.remove(address);
        LOG.info("STOPPED monitoring peer at {}", address);
    }

    public Future<Double> getAverageDelay(InetSocketAddress address) {
        return resultExecutor.submit(() -> {
            DescriptiveStatistics stats = monitoredPeers.get(address);
            if (stats != null) {
                return (stats.getMean() / 2) / 1000;
            } else {
                return null;
            }
        });
    }

    public Future<DescriptiveStatistics> getStats(InetSocketAddress address) {
        return resultExecutor.submit(() -> {
            DescriptiveStatistics stats = monitoredPeers.get(address);
            if (stats != null) {
                return stats.copy();
            } else {
                return null;
            }
        });
    }
}
