package io.moquette.delaygrouping;

import io.moquette.delaygrouping.anchor.AnchorConnection;
import io.moquette.delaygrouping.monitoring.ConnectionMonitor;
import io.moquette.delaygrouping.peering.PeerConnection;
import io.moquette.delaygrouping.peering.PeerConnectionManager;
import io.moquette.delaygrouping.peering.messaging.*;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
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
    private ConnectionMonitor connectionMonitor;
    private Set<InetAddress> previousLeaders;
    private InetAddress leader;
    private PeerConnectionManager peerConnectionManager;
    private int leaderCapabilityMeasure = new Random().nextInt(); // TODO Add this to config?
    private Set<InetAddress> groupMembers = ConcurrentHashMap.newKeySet();
    private SubscriptionStore groupSubscriptions = new SubscriptionStore();
    private SubscriptionStore clientSubscriptions = new SubscriptionStore();

    public DelaygroupingOrchestrator(DelaygroupingConfiguration config, BiConsumer<MqttPublishMessage, String> internalPublishFunction) {
        this.internalPublishFunction = internalPublishFunction;
        cloudAnchor = config.getAnchorNodeAddress();
        latencyThreshold = config.getLatencyThreshold();
        this.localInterfaceAddress = config.getHost();
        clientId = localInterfaceAddress.getHostAddress();
        this.connectionMonitor = new ConnectionMonitor(1, 20);
        peerConnectionManager = new PeerConnectionManager(localInterfaceAddress, this::handleNewPeerConnection);
        leader = null;

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
            // TODO Should we sleep?
            try {
                Thread.sleep(100);
            } catch (InterruptedException ignored) {
            }
        }

        // TODO Should we already accept client connections? We don't really have a choice, have we?

        // TODO What if another node joins while two are already negotiating? --> Just deny (we might need a retry mechanism here)

    }

    private void doBootstrap() {
        transitionToLeader();
    }

    private void transitionToLeader() {
        leader = null;

        anchorConnection = new AnchorConnection(cloudAnchor, localInterfaceAddress);
        anchorConnection.startLeaderAnnouncement();
        // TODO send client subscriptions to cloud anchor

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
            } catch (InterruptedException | ExecutionException ignored) {
            }
        }

        if (minimumDelayLeader != null) {
            // if we have a candidate start negotiation (see handlers)
            var newLeaderConnection = peerConnectionManager.getConnectionToPeer(minimumDelayLeader);
            newLeaderConnection.registerMessageHandler(this::handleMembershipMessages, PeerMessageType.MEMBERSHIP);
            newLeaderConnection.sendMessage(PeerMessageMembership.join(leaderCapabilityMeasure));
        }
    }

    private void transitionToNonLeader(InetAddress newLeader) {
        leader = newLeader;

        // stop all leader-related functions
        connectionMonitor.removeAll();
        anchorConnection.shutdown();
        anchorConnection = null;

        connectionMonitor.addMonitoredPeer(newLeader);
        sendMessageToGroup(PeerMessageRedirect.redirect(newLeader));

        groupMembers.clear();
        groupSubscriptions.clear();

        state = OrchestratorState.NON_LEADER;
    }

    private void doNonLeader() {
        // monitor latency to group leader and switch to leader (effectively leaving group)
        Double leaderDelay = null;
        try {
            leaderDelay = connectionMonitor.getAverageDelay(leader).get();
            if (leaderDelay > latencyThreshold) {
                transitionToLeader();
            }
        } catch (InterruptedException | ExecutionException ignored) {
        }
    }

    private void handleMembershipMessages(PeerMessage msg, PeerConnection origin) {
        PeerMessageMembership message = (PeerMessageMembership) msg;
        switch (message.getSignal()) {
            case JOIN:
                // Only allow join if we are a leader
                if (state.equals(OrchestratorState.LEADER)) {
                    if (leaderCapabilityMeasure >= message.getElectionValue()) {
                        // We'll continue to be the leader and just add a new member
                        origin.sendMessage(PeerMessageMembership.joinAck(false));
                        origin.sendMessage(PeerMessageMembership.groupUpdate(null, new ArrayList<>(groupMembers)));
                        sendMessageToGroup(PeerMessageMembership.groupUpdate(null, Arrays.asList(origin.getRemoteAddress())));
                        groupMembers.add(origin.getRemoteAddress());
                    } else {
                        // We'll let the new peer be the leader
                        origin.sendMessage(PeerMessageMembership.joinAck(true));
                        transitionToNonLeader(origin.getRemoteAddress());
                    }
                } else {
                    // we're not leader so deny join, the peer that tried to join our group should reevaluate its leader list
                    origin.sendMessage(PeerMessageMembership.deny());
                }
                break;
            case JOIN_ACK:
                // We are allowed to join!
                if (message.isShouldBeLeader()) {
                    if (state.equals(OrchestratorState.NON_LEADER)) {
                        transitionToLeader();
                    }
                } else {
                    if (state.equals(OrchestratorState.LEADER)) {
                        // we join the other group and should not be leader
                        transitionToNonLeader(origin.getRemoteAddress());
                    }
                }
                break;
            case LEAVE:
                if (state.equals(OrchestratorState.LEADER)) {
                    peerConnectionManager.closeConnectionToPeer(message.getLeavingPeer());
                    groupMembers.remove(message.getLeavingPeer());
                    sendMessageToGroup(PeerMessageMembership.groupUpdate(Arrays.asList(message.getLeavingPeer()), null));
                }
                // Ignore LEAVE if we're not leader
                break;
            case GROUP_UPDATE:
                if (state.equals(OrchestratorState.NON_LEADER)) {
                    groupMembers.removeAll(message.getLeftPeers());
                    groupMembers.addAll(message.getJoinedPeers());
                }
                // Ignore GROUP_UPDATE if we're leader
                break;
            // TODO We might need a JOIN_DONE / JOIN_ACKACK to make sure we don't start migrating peers when the new leader is not ready yet
        }
    }

    private void handleRedirectMessages(PeerMessage msg, PeerConnection origin) {
        peerConnectionManager.closeConnectionToPeer(origin.getRemoteAddress());

        if (state.equals(OrchestratorState.NON_LEADER)) {
            // connect to new leader (we are being migrated)
            var message = (PeerMessageRedirect) msg;
            peerConnectionManager.getConnectionToPeer(message.getTarget());
            // TODO resend all client subscriptions (this must happen after successful join, we need to keep track of that)
        }
        // ignore redirects if we are a leader
    }

    private void handlePublishMessages(PeerMessage msg, PeerConnection origin) {
        var message = (PeerMessagePublish) msg;
        message.getPublishMessages().forEach(this::internalPublish);
        if (state.equals(OrchestratorState.LEADER)) {
            anchorConnection.publish(message);
        }
    }

    private void handleSubscribeMessages(PeerMessage msg, PeerConnection origin) {
        var message = (PeerMessageSubscribe) msg;
        groupSubscriptions.addSubscription(origin.getRemoteAddress().getHostAddress(), message.getTopicFilter());
        // TODO Forward subscription to cloud anchor if leader
    }

    private void handleNewPeerConnection(PeerConnection connection) {
        // TODO We might wanna serialize event processing (at least membership messages)
        // this is run synchronously in channel initializer, so don't do anything costly here!
        connection.registerMessageHandler(this::handleMembershipMessages, PeerMessageType.MEMBERSHIP);
        connection.registerMessageHandler(this::handleRedirectMessages, PeerMessageType.REDIRECT);
        connection.registerMessageHandler(this::handlePublishMessages, PeerMessageType.PUBLISH);
        connection.registerMessageHandler(this::handleSubscribeMessages, PeerMessageType.SUBSCRIBE);
    }

    private void handleInterceptedPublish(MqttPublishMessage interceptedMsg) {
        if (state.equals(OrchestratorState.LEADER)) {
            anchorConnection.publish(interceptedMsg);
        }
        if (groupSubscriptions.matches(interceptedMsg.variableHeader().topicName())) {
            sendMessageToGroup(PeerMessagePublish.fromMessage(interceptedMsg));
        }
    }

    private void handleInterceptedSubscribe(String topicFilter) {
        // We need to keep track of our clients subscriptions for group migration (to send our subscriptions to the new leader)
        clientSubscriptions.addSubscription(clientId, topicFilter);

        sendMessageToGroup(PeerMessageSubscribe.fromTopicFilter(topicFilter));

        if (state.equals(OrchestratorState.LEADER)) {
            // TODO Forward subscription to cloud anchor if leader (implement subscribe on cloud anchor)

        }
    }

    private void handleAnchorPublishMessage() {
        // TODO Check if this is properly wired
        // TODO Do internal publish
        // TODO Forward publish to all group members (if subscriptions match)
    }

    private void sendMessageToGroup(PeerMessage msg) {
        groupMembers.forEach(member -> {
            // Don't send to ourselves
            if (!member.equals(localInterfaceAddress)) {
                peerConnectionManager.getConnectionToPeer(member).sendMessage(msg);
            }
        });
    }

    private void internalPublish(MqttPublishMessage msg) {
        internalPublishFunction.accept(msg, clientId);
    }

    public Consumer<MqttPublishMessage> getInterceptHandler() {
        return this::handleInterceptedPublish;
    }

    public Consumer<String> getSubscribeHandler() {
        return this::handleInterceptedSubscribe;
    }

    private enum OrchestratorState {
        BOOTSTRAP,
        LEADER,
        NON_LEADER,
    }
}
