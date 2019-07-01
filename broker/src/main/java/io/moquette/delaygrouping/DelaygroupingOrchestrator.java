package io.moquette.delaygrouping;

import io.moquette.delaygrouping.anchor.AnchorConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

public class DelaygroupingOrchestrator {
    private static final Logger LOG = LoggerFactory.getLogger(DelaygroupingOrchestrator.class);

    private InetSocketAddress cloudAnchor;
    private int latencyThreshold;
    private AnchorConnection anchorConnection;
    private String clientId;

    public DelaygroupingOrchestrator(DelaygroupingConfiguration config) {
        cloudAnchor = config.getAnchorNodeAddress();
        latencyThreshold = config.getLatencyThreshold();

        transitionToLeader();

        // Enter LEADER mode
        // Connect to anchor node
        // TODO Should we already accept client connections?
        // Do an MQTT SUB on the /$SYS/COLLIN/leaders topic

        // Build a list of other leaders
        // Add them to monitoring
        // Check if any other leader is below latencyThreshold

        // Connect to the leader with the lowest delay
        // "Hello, I'd like to join your group and I do want to be the leader / I don't want to be a leader"
        // Do leader election between the two
        // Either doLeader or doNonLeader as a result

        // TODO What if another node joins while two are already negotiating? --> Wait and then redirect to new leader if necessary

    }

    private void doBootstrap() {

    }

    private void transitionToLeader() {
        //anchorConnection = new AnchorConnection(cloudAnchor);

        doLeader();
    }

    private void doLeader() {
        // Publish IP on /$SYS/COLLIN/leaders topic
        // Check other leaders for nodes below the threshold

        // Collect subscriptions and forward them to cloud anchor

        // Forward matching PUBs from cloud anchor to group members

        // Forward all PUBs from group members to cloud
    }

    private void transitionToNonLeader() {
        // send subscriptions to new leader
    }

    private void doNonLeader() {
        //
    }
}
