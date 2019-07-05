package io.moquette.delaygrouping;

import io.moquette.delaygrouping.anchor.AnchorConnection;
import io.moquette.delaygrouping.anchor.Message;
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
import java.util.concurrent.*;
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
    // TODO Check when to reset group / client subscriptions?!
    private SubscriptionStore groupSubscriptions = new SubscriptionStore();
    private SubscriptionStore clientSubscriptions = new SubscriptionStore();
    private ScheduledExecutorService peerMessagingExecutor = Executors.newSingleThreadScheduledExecutor();

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
        if (leader != null) {
            sendMessageToLeader(PeerMessageMembership.leave(localInterfaceAddress));
        }
        leader = null;

        anchorConnection = new AnchorConnection(cloudAnchor, localInterfaceAddress);
        anchorConnection.startLeaderAnnouncement();
        anchorConnection.setMqttCallback(this::handleAnchorPublishMessage);
        clientSubscriptions.getFlattened().forEach(topicFilter -> anchorConnection.addSubscription(topicFilter));

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
            newLeaderConnection.sendMessage(PeerMessageMembership.join(leaderCapabilityMeasure));
        }
    }

    private void transitionToNonLeader(InetAddress newLeader) {
        // stop all leader-related functions
        connectionMonitor.removeAll();
        anchorConnection.shutdown();
        anchorConnection = null;

        // connect to new leader
        leader = newLeader;
        connectionMonitor.addMonitoredPeer(newLeader);

        state = OrchestratorState.NON_LEADER;

        // TODO This seems a bit risky. Have all clients do a full join with leader negotiation instead?
        // transmit state to new leader
        var subscriptions = new HashSet<>(groupSubscriptions.getFlattened());
        subscriptions.addAll(clientSubscriptions.getFlattened());
        sendMessageToLeader(PeerMessageSubscribe.fromTopicFilter(subscriptions));
        sendMessageToLeader(PeerMessageMembership.groupUpdate(null, new ArrayList<>(groupMembers)));

        // point group members to new leader
        sendMessageToGroup(PeerMessageRedirect.redirect(newLeader));
        groupMembers.clear();
        groupSubscriptions.clear();
    }

    private void doNonLeader() {
        // monitor latency to group leader and switch to leader (effectively leaving group)
        Double leaderDelay;
        try {
            leaderDelay = connectionMonitor.getAverageDelay(leader).get();
            if (leaderDelay > latencyThreshold) {
                transitionToLeader();
            }
        } catch (InterruptedException | ExecutionException ignored) {
        }
    }

    private void handleMembershipMessages(PeerMessageMembership msg, PeerConnection origin) {
        switch (msg.getSignal()) {
            case JOIN:
                // Only allow join if we are a leader
                if (state.equals(OrchestratorState.LEADER)) {
                    if (leaderCapabilityMeasure >= msg.getElectionValue()) {
                        // We'll continue to be the leader and just add a new member
                        origin.sendMessage(PeerMessageMembership.joinAck(false));
                        origin.sendMessage(PeerMessageMembership.groupUpdate(null, new ArrayList<>(groupMembers)));

                    } else {
                        // We'll let the new peer be the leader
                        origin.sendMessage(PeerMessageMembership.joinAck(true));
                    }
                } else {
                    // we're not leader so deny join, the peer that tried to join our group should reevaluate its leader list
                    origin.sendMessage(PeerMessageMembership.deny());
                }
                break;
            case JOIN_ACK:
                // We are allowed to join!
                if (msg.isShouldBeLeader()) {
                    if (state.equals(OrchestratorState.NON_LEADER)) {
                        transitionToLeader();
                        origin.sendMessage(PeerMessageMembership.joinAckAck(msg));
                    }
                } else {
                    if (state.equals(OrchestratorState.LEADER)) {
                        // we join the other group and should not be leader
                        transitionToNonLeader(origin.getRemoteAddress());
                        origin.sendMessage(PeerMessageMembership.joinAckAck(msg));
                    }
                }
                break;
            case JOIN_ACKACK:
                if (state.equals(OrchestratorState.LEADER)) {
                    if (msg.isShouldBeLeader()) {
                        // the other peer joins as client so update our group
                        sendMessageToGroup(PeerMessageMembership.groupUpdate(null, Arrays.asList(origin.getRemoteAddress())));
                        groupMembers.add(origin.getRemoteAddress());
                    } else {
                        // the other peer confirms that he switched to leader mode and is therefore ready to accept peers (including ourselves)
                        transitionToNonLeader(origin.getRemoteAddress());
                    }
                }
                // ignore JOIN_ACKACK if we're not leader
                break;
            case BUSY:
                // Schedule retry after random wait time
                peerMessagingExecutor.schedule(
                    () -> origin.sendMessage(PeerMessageMembership.join(leaderCapabilityMeasure)),
                    new Random().nextInt(1000), TimeUnit.MILLISECONDS);
                break;
            case DENY:
                // Do nothing and just let the main loop do its thing
                break;
            case LEAVE:
                if (state.equals(OrchestratorState.LEADER)) {
                    peerConnectionManager.closeConnectionToPeer(msg.getLeavingPeer());
                    groupMembers.remove(msg.getLeavingPeer());
                    sendMessageToGroup(PeerMessageMembership.groupUpdate(Arrays.asList(msg.getLeavingPeer()), null));
                }
                // Ignore LEAVE if we're not leader
                break;
            case GROUP_UPDATE:
                if (state.equals(OrchestratorState.NON_LEADER)) {
                    groupMembers.removeAll(msg.getLeftPeers());
                    groupMembers.addAll(msg.getJoinedPeers());
                }
                // Ignore GROUP_UPDATE if we're leader
                break;
            // TODO Deny all group modifications while process is ongoing (we might indeed need an additional ack for that... and some automatic retrying)
        }
    }

    private void handleRedirectMessages(PeerMessageRedirect msg, PeerConnection origin) {
        if (state.equals(OrchestratorState.NON_LEADER)) {
            peerConnectionManager.closeConnectionToPeer(leader);
            connectionMonitor.removeAll();
            leader = msg.getTarget();
            peerConnectionManager.getConnectionToPeer(leader);
            connectionMonitor.addMonitoredPeer(leader);
            // Lets not do the whole JOIN thing for now and just trust in the new leader having received all its leader state (although it would probably be more correct, i.e. safer)
        }
        // ignore REDIRECT if we're leader (as its supposed to steer non-leaders to a new leader)
    }

    private void handlePublishMessages(PeerMessagePublish msg, PeerConnection origin) {
        msg.getPublishMessages().forEach(this::internalPublish);
        if (state.equals(OrchestratorState.LEADER)) {
            anchorConnection.publish(msg);
        }
    }

    private void handleSubscribeMessages(PeerMessageSubscribe msg, PeerConnection origin) {
        msg.getTopicFilters().forEach(topicFilter ->
            groupSubscriptions.addSubscription(origin.getRemoteAddress().getHostAddress(), topicFilter));

        if (state.equals(OrchestratorState.LEADER)) {
            msg.getTopicFilters().forEach(topicFilter -> anchorConnection.addSubscription(topicFilter));
            sendMessageToGroup(msg);
        }
    }

    private void handlePeerMessage(PeerMessage msg, PeerConnection origin) {
        switch (msg.type) {
            case PUBLISH:
                handlePublishMessages((PeerMessagePublish) msg, origin);
            case REDIRECT:
                peerMessagingExecutor.execute(() -> handleRedirectMessages((PeerMessageRedirect) msg, origin));
            case SUBSCRIBE:
                handleSubscribeMessages((PeerMessageSubscribe) msg, origin);
            case MEMBERSHIP:
                peerMessagingExecutor.execute(() -> handleMembershipMessages((PeerMessageMembership) msg, origin));
        }
    }

    private void handleNewPeerConnection(PeerConnection connection) {
        // this is run synchronously in channel initializer, so don't do anything costly here!
        connection.registerMessageHandler(this::handlePeerMessage,
            PeerMessageType.SUBSCRIBE, PeerMessageType.PUBLISH, PeerMessageType.REDIRECT, PeerMessageType.MEMBERSHIP);
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

        if (state.equals(OrchestratorState.NON_LEADER)) {
            // Only send publish to leader as it will get relayed by the leader
            sendMessageToLeader(PeerMessageSubscribe.fromTopicFilter(Arrays.asList(topicFilter)));
        } else if (state.equals(OrchestratorState.LEADER)) {
            anchorConnection.addSubscription(topicFilter);
        }
    }

    private void handleAnchorPublishMessage(Message msg) {
        var mqttPubMsg = Utils.mqttPublishMessageFromValues(msg.topic, msg.payload);
        internalPublish(mqttPubMsg);
        if (groupSubscriptions.matches(msg.topic)) {
            sendMessageToGroup(PeerMessagePublish.fromMessage(mqttPubMsg));
        }
    }

    private void sendMessageToLeader(PeerMessage msg) {
        if (state.equals(OrchestratorState.LEADER) && leader != null) {
            peerConnectionManager.getConnectionToPeer(leader).sendMessage(msg);
        } else {
            LOG.error("Error while trying to send message to leader! Leader not known or not in NON_LEADER state. Message: {}", msg);
        }
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
