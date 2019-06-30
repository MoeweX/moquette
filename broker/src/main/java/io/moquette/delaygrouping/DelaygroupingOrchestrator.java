package io.moquette.delaygrouping;

import io.moquette.delaygrouping.mqtt.MqttConnection;

import java.net.InetSocketAddress;

public class DelaygroupingOrchestrator {

    private InetSocketAddress cloudAnchor;
    private int latencyThreshold;

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

        // TODO What if another node joins while two are already negotiating? Put on hold for now? Redirect to new leader?
        // TODO How about all nodes can redirect to their leader?

    }

    private void doBootstrap() {

    }

    private void transitionToLeader() {
        // What should happen if connection to anchor node is not possible?
        //var anchorConnection = new MqttConnection();

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
