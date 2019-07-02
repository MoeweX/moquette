package io.moquette.delaygrouping;

import io.moquette.delaygrouping.anchor.AnchorConnection;
import io.moquette.delaygrouping.monitoring.ConnectionMonitor;
import io.moquette.delaygrouping.peering.PeerConnection;
import io.moquette.delaygrouping.peering.PeerConnectionManager;
import io.moquette.delaygrouping.peering.messaging.PeerMessage;
import io.moquette.delaygrouping.peering.messaging.PeerMessageMembership;
import io.moquette.delaygrouping.peering.messaging.PeerMessageMembership.MembershipSignal;
import io.moquette.delaygrouping.peering.messaging.PeerMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class DelaygroupingOrchestrator {
    private static final Logger LOG = LoggerFactory.getLogger(DelaygroupingOrchestrator.class);

    private InetSocketAddress cloudAnchor;
    private InetAddress localInterfaceAddress;
    private int latencyThreshold;
    private AnchorConnection anchorConnection;
    private String clientId;
    private BiConsumer<MqttPublishMessage, String> internalPublishFunction;
    private OrchestratorState state;
    private ConnectionMonitor connectionMonitor;
    private Set<InetAddress> previousLeaders;
    private InetAddress leader;
    private boolean redirectPeers;
    private PeerConnectionManager peerConnectionManager;
    private int leaderCapabilityMeasure = new Random().nextInt();

    public DelaygroupingOrchestrator(DelaygroupingConfiguration config, BiConsumer<MqttPublishMessage, String> internalPublishFunction) {
        this.internalPublishFunction = internalPublishFunction;
        cloudAnchor = config.getAnchorNodeAddress();
        latencyThreshold = config.getLatencyThreshold();
        this.localInterfaceAddress = config.getHost();
        this.connectionMonitor = new ConnectionMonitor(1, 20);
        peerConnectionManager = new PeerConnectionManager(localInterfaceAddress, this::handleNewPeerConnection);
        leader = null;
        redirectPeers = false;

        state = OrchestratorState.BOOTSTRAP;

        // TODO How about a decent shutdown procedure?
        while (true) {
            switch (state) {
                case BOOTSTRAP:
                    doBootstrap();
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


        transitionToLeader();
    }

    private void transitionToLeader() {
        anchorConnection = new AnchorConnection(cloudAnchor, localInterfaceAddress);
        anchorConnection.startLeaderAnnouncement();

        leader = null;
        redirectPeers = false;

        previousLeaders = new HashSet<>();

        state = OrchestratorState.LEADER;
    }

    private void doLeader() {
        // Check other leaders and update connection monitoring
        var currentLeaders = anchorConnection.getCollectedLeaders();
        previousLeaders.removeAll(currentLeaders);
        currentLeaders.forEach(leader -> connectionMonitor.addMonitoredPeer(leader));
        previousLeaders.forEach(leader -> connectionMonitor.removeMonitoredPeer(leader));
        previousLeaders = currentLeaders;

        // Check if there are leaders below the set threshold
        var minimumDelay = Double.MAX_VALUE;
        InetAddress minimumDelayLeader = null;
        for (InetAddress leader : currentLeaders) {
            try {
                var averageDelay = connectionMonitor.getAverageDelay(leader).get();
                if (averageDelay < latencyThreshold && averageDelay < minimumDelay) {
                    minimumDelay = averageDelay;
                    minimumDelayLeader = leader;
                }
            } catch (InterruptedException | ExecutionException ignored) {}
        }

        if (minimumDelayLeader != null) {
            // if we have a candidate start negotiation (see handlers)
            var newLeaderConnection = peerConnectionManager.getConnectionToPeer(minimumDelayLeader);
            newLeaderConnection.registerMessageHandler(this::handleMembershipMessages, PeerMessageType.MEMBERSHIP);
            newLeaderConnection.sendMessage(PeerMessageMembership.join());
        }

        // Collect subscriptions and forward them to cloud anchor

        // Forward matching PUBs from cloud anchor to group members

        // Forward all PUBs from group members to cloud
    }

    private void transitionToNonLeader() {
        // stop all leader-related functions
        connectionMonitor.removeAll();
        anchorConnection.shutdown();
        anchorConnection = null;
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

    private void handleMembershipMessages(PeerMessage msg, PeerConnection origin) {
        PeerMessageMembership message = (PeerMessageMembership) msg;
        switch (message.getSignal()) {
            case JOIN:
                break;
            case JOIN_ACK:
                origin.sendMessage(PeerMessageMembership.electionRequest(leaderCapabilityMeasure));
                break;
            case ELECTION_REQUEST:
                if (leaderCapabilityMeasure >= message.getElectionValue()) {
                    origin.sendMessage(PeerMessageMembership.electionResponse(false));
                } else {
                    origin.sendMessage(PeerMessageMembership.electionResponse(true));
                }
                break;
            case ELECTION_RESPONSE:
                break;
        }
    }

    private void handleNewPeerConnection(PeerConnection connection) {
        connection.registerMessageHandler(this::handleMembershipMessages, PeerMessageType.MEMBERSHIP);
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
