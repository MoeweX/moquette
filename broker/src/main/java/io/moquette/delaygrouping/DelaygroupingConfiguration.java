package io.moquette.delaygrouping;

import io.moquette.server.config.IConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

public class DelaygroupingConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(DelaygroupingConfiguration.class);

    private InetAddress host;
    private int port;
    private InetSocketAddress anchorNodeAddress;
    private int latencyThreshold;
    private boolean valid;

    public DelaygroupingConfiguration(IConfig config) {
        valid = true;

        var hostString = config.getProperty("delaygrouping_peering_host", "127.0.0.1");
        try {
            host = InetAddress.getByName(hostString);
        } catch (UnknownHostException e) {
            LOG.error("Invalid local peering interface address! Skipping activation. Exception: {}", e);
            valid = false;
        }

        port = config.intProp("delaygrouping_peering_port", 1884);

        latencyThreshold = config.intProp("delaygrouping_threshold", 5);

        var anchorNodeAddressValue = config.getProperty("delaygrouping_anchor_node_address");
        if (anchorNodeAddressValue != null) {
            String[] fields = anchorNodeAddressValue.split(":");
            anchorNodeAddress = new InetSocketAddress(fields[0], Integer.valueOf(fields[1]));
        } else {
            valid = false;
        }
    }

    public InetAddress getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public InetSocketAddress getAnchorNodeAddress() {
        return anchorNodeAddress;
    }

    public boolean isEnabled() {
        return valid;
    }

    public int getLatencyThreshold() {
        return latencyThreshold;
    }
}
