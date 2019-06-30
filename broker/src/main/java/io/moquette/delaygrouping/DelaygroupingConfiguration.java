package io.moquette.delaygrouping;

import io.moquette.server.config.IConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

public class DelaygroupingConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(DelaygroupingConfiguration.class);

    private final String host;
    private final int port;
    private final InetSocketAddress anchorNodeAddress;
    private final int latencyThreshold;

    public DelaygroupingConfiguration(IConfig config) {
        host = config.getProperty("delaygrouping_host", "127.0.0.1");
        port = config.intProp("delaygrouping_port", 1884);
        latencyThreshold = config.intProp("delaygrouping_threshold", 5);

        var anchorNodeAddressValue = config.getProperty("delaygrouping_anchor_node_address");
        if (anchorNodeAddressValue != null) {
            String[] fields = anchorNodeAddressValue.split(":");
            anchorNodeAddress = new InetSocketAddress(fields[0], Integer.valueOf(fields[1]));
        } else {
            anchorNodeAddress = null;
        }
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public InetSocketAddress getAnchorNodeAddress() {
        return anchorNodeAddress;
    }

    public boolean isEnabled() {
        return anchorNodeAddress != null;
    }

    public int getLatencyThreshold() {
        return latencyThreshold;
    }
}
