package io.moquette.delaygrouping.monitoring;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ConnectionMonitor {

    private static final Logger LOG = LoggerFactory.getLogger(ConnectionMonitor.class);
    private final int interval;
    private final int windowSize;
    private final String pingRegex = "icmp_seq=\\d+ ttl=\\d+ time=([\\d\\.]+) ms";
    private final Pattern pingPattern = Pattern.compile(pingRegex, Pattern.MULTILINE);
    private ConcurrentHashMap<InetAddress, DescriptiveStatistics> monitoredPeers = new ConcurrentHashMap<>();
    private ExecutorService executor = Executors.newSingleThreadExecutor();
    private Map<InetAddress, Process> runningInstances = new ConcurrentHashMap<>();
    private ProcessBuilder processBuilder = new ProcessBuilder();

    /**
     * Set up connection monitor.
     * @param interval Monitoring interval in ms.
     * @param windowSize Window size for averaging.
     */
    public ConnectionMonitor(int interval, int windowSize) {
        if (interval < 200) throw new IllegalArgumentException("Interval must be >= 200ms!");
        this.interval = interval;
        this.windowSize = windowSize;

        LOG.info("Starting connection monitor...");

        // maybe add some cyclic result collection...
    }

    private void collectResults(InetAddress address) {
        // only call from executor!
        Process instance = runningInstances.get(address);
        if (instance == null) return;

        InputStream pingStdOut = instance.getInputStream();
        StringBuilder accumulator = new StringBuilder();
        char character;
        var marked = false;

        try {
            while (pingStdOut.available() > 0) {
                character = (char) pingStdOut.read();
                if (character == 10) {
                    pingStdOut.mark(1000);
                    marked = true;
                }
                accumulator.append(character);
            }
            if (marked) pingStdOut.reset();
        } catch (IOException ex) {
            LOG.error("IOException while reading stdOut! Accumulator content: {}", accumulator.toString(), ex);
        }

        List<String> outputLines = Arrays.asList(accumulator.toString().split("\n"));
        DescriptiveStatistics peerStats = monitoredPeers.get(address);

        outputLines.forEach(line -> {
            Matcher matcher = pingPattern.matcher(line);
            if (matcher.find()) {
                String delay = matcher.group(1);
                try {
                    peerStats.addValue(Double.valueOf(delay));
                } catch (NumberFormatException ignored) {
                }
            }
        });
    }

    public void shutdown() {
        // kill all running ping instances
        runningInstances.forEach((address, process) -> process.destroy());
    }

    public void addMonitoredPeer(InetAddress address) {
        if (monitoredPeers.containsKey(address)) return;

        Process instance = createProcess(address);
        if (instance != null) {
            monitoredPeers.put(address, new DescriptiveStatistics(windowSize));
            runningInstances.put(address, instance);

            LOG.info("STARTED monitoring peer at {}", address);
        }

    }

    public void removeMonitoredPeer(InetAddress address) {
        monitoredPeers.remove(address);
        Process instance = runningInstances.remove(address);
        if (instance != null) {
            instance.destroy();
        }

        LOG.info("STOPPED monitoring peer at {}", address);
    }

    public void removeAll() {
        monitoredPeers.clear();
        runningInstances.forEach((address, process) -> process.destroy());
        runningInstances.clear();

        LOG.info("STOPPED monitoring ALL peers");
    }

    private Process createProcess(InetAddress address) {
        processBuilder.command("ping", "-n", "-i", String.valueOf(interval / 1000), address.getHostAddress());
        try {
            return processBuilder.start();
        } catch (IOException e) {
            LOG.error("Error creating ping instance: {}", e);
            return null;
        }
    }

    public Future<Double> getAverageDelay(InetAddress address) {
        return executor.submit(() -> {
            collectResults(address);
            DescriptiveStatistics stats = monitoredPeers.get(address);
            if (stats != null) {
                return stats.getMean() / 2d;
            } else {
                return null;
            }
        });
    }

    public Future<DescriptiveStatistics> getStats(InetAddress address) {
        return executor.submit(() -> {
            collectResults(address);
            DescriptiveStatistics stats = monitoredPeers.get(address);
            if (stats != null) {
                return stats.copy();
            } else {
                return null;
            }
        });
    }
}
