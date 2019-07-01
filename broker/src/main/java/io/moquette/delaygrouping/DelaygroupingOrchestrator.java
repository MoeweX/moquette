package io.moquette.delaygrouping;

import io.moquette.delaygrouping.anchor.AnchorConnection;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class DelaygroupingOrchestrator {
    private static final Logger LOG = LoggerFactory.getLogger(DelaygroupingOrchestrator.class);

    private InetSocketAddress cloudAnchor;
    private InetAddress localInterfaceAddress;
    private int latencyThreshold;
    private AnchorConnection anchorConnection;
    private String clientId;
    private BiConsumer<MqttPublishMessage, String> internalPublishFunction;
    private OrchestratorState state;

    public DelaygroupingOrchestrator(DelaygroupingConfiguration config, BiConsumer<MqttPublishMessage, String> internalPublishFunction) {
        this.internalPublishFunction = internalPublishFunction;
        cloudAnchor = config.getAnchorNodeAddress();
        latencyThreshold = config.getLatencyThreshold();
        this.localInterfaceAddress = config.getHost();

        state = OrchestratorState.BOOTSTRAP;

        // TODO How about a decent shutdown procedure?
        while (true) {
            switch (state) {
                case BOOTSTRAP:
                    transitionToLeader();
                    break;
                case LEADER:
                    doLeader();
                    break;
                case NON_LEADER:
                    doNonLeader();
                    break;
            }
        }

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
        anchorConnection = new AnchorConnection(cloudAnchor, localInterfaceAddress);
        anchorConnection.startLeaderAnnouncement();

        state = OrchestratorState.LEADER;
    }

    private void doLeader() {
        // Check other leaders for nodes below the threshold

        // Collect subscriptions and forward them to cloud anchor

        // Forward matching PUBs from cloud anchor to group members

        // Forward all PUBs from group members to cloud
    }

    private void transitionToNonLeader() {
        // shutdown anchor connection (do we need to keep something in mind?)
        // redirect all clients to new leader
        // enable automatic redirection of connecting peers to leader
        // send subscriptions to new leader


        state = OrchestratorState.NON_LEADER;
    }

    private void doNonLeader() {
        // forward all group internal publish messages to all other group members
        // forward all other publish messages (i.e. no group internal subscribers) to leader
        // forward subscribe messages to all group members
        // TODO monitor latency to group leader and break connection if threshold exceeded (is that a good idea?)
    }

    private void handleInterceptedPublish(MqttPublishMessage interceptedMsg) {
        // TODO Implement... :-)
    }

    private void handleInterceptedSubscribe(String topicFilter) {
        // TODO Implement...
    }

    public Consumer<MqttPublishMessage> getInterceptHandler() {
        return this::handleInterceptedPublish;
    }

    public Consumer<String> getSubscibeHandler() {
        return this::handleInterceptedSubscribe;
    }

    private enum OrchestratorState {
        BOOTSTRAP,
        LEADER,
        NON_LEADER,
    }
}
