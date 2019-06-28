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

    public DelaygroupingConfiguration(IConfig config) {
        host = config.getProperty("peering_host", "127.0.0.1");
        port = config.intProp("peering_port", 1884);

        String[] fields = config.getProperty("peering_anchor_node_address").split(":");
        anchorNodeAddress = new InetSocketAddress(fields[0], Integer.valueOf(fields[1]));
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
}
