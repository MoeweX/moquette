package io.moquette.bridge;

import io.moquette.BrokerConstants;
import io.moquette.server.config.IConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class BridgeConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(BridgeConfiguration.class);

    private final List<InetSocketAddress> bridgeSocketAddresses;
    private final String host;
    private final int port;

    public BridgeConfiguration(IConfig config) {
        this.bridgeSocketAddresses = new ArrayList<>();

        List<String> bridgeConnections = config.getPropertyAsList(BrokerConstants.BRIDGE_CONNECTIONS_PROPERTY_NAME);
        if (!bridgeConnections.isEmpty()) {
            bridgeSocketAddresses.addAll(bridgeConnections.stream()
                .map(c -> {
                    String[] parts = c.split(":");
                    try {
                        InetAddress host = InetAddress.getByName(parts[0]);
                        int port = Integer.decode(parts[1]);
                        return new InetSocketAddress(host, port);
                    } catch (UnknownHostException ex) {
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList()));
        }

        host = config.getProperty("bridge_host", null);
        port = config.intProp("bridge_port", 0);

        LOG.info("Configured bridge peers: {}", bridgeConnections);
    }

    public List<InetSocketAddress> getBridgePeers() {
        return this.bridgeSocketAddresses;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }
}
