package io.moquette.delaygrouping;

import io.moquette.server.config.IConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Random;

public class DelaygroupingConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(DelaygroupingConfiguration.class);
    private InetAddress name;
    private InetAddress bindHost;
    private int port;
    private InetSocketAddress anchorNodeAddress;
    private int latencyThreshold;
    private boolean valid;
    private int leadershipCapabilityMeasure;
    private String clientId;

    public DelaygroupingConfiguration(IConfig config) {
        valid = true;

        var nameString = config.getProperty("delaygrouping_peering_name");
        if (nameString == null) {
            LOG.error("No delaygrouping_peering_name configured. Skipping delaygrouping activation.");
            valid = false;
            return;
        } else {
            try {
                name = InetAddress.getByName(nameString);
            } catch (UnknownHostException e) {
                LOG.error("Invalid delaygrouping_peering_name! Skipping activation. Exception: {}", e);
                valid = false;
                return;
            }
        }

        var bindHostString = config.getProperty("delaygrouping_peering_bind_host", "0.0.0.0");
        try {
            bindHost = InetAddress.getByName(bindHostString);
        } catch (UnknownHostException e) {
            LOG.error("Invalid delaygrouping_peering_bind_host! Skipping activation. Exception: {}", e);
            valid = false;
            return;
        }

        port = config.intProp("delaygrouping_peering_port", 1884);

        latencyThreshold = config.intProp("delaygrouping_threshold", 5);

        leadershipCapabilityMeasure = config.intProp("delaygrouping_leadership_capability_measure", new Random().nextInt());

        clientId = config.getProperty("delaygrouping_client_id", name.getHostAddress());

        var anchorNodeAddressValue = config.getProperty("delaygrouping_anchor_node_address");
        if (anchorNodeAddressValue != null) {
            String[] fields = anchorNodeAddressValue.split(":");
            anchorNodeAddress = new InetSocketAddress(fields[0], Integer.valueOf(fields[1]));
        } else {
            valid = false;
        }
    }

    public InetAddress getName() {
        return name;
    }

    public InetAddress getBindHost() {
        return bindHost;
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

    public int getLeadershipCapabilityMeasure() {
        return leadershipCapabilityMeasure;
    }

    public String getClientId() {
        return clientId;
    }
}
