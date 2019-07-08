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
    private SubscriptionStore clientSubscriptions = new SubscriptionStore(); // TODO We never unsubscribe...
    private ScheduledExecutorService peerMessagingExecutor = Executors.newSingleThreadScheduledExecutor();
    private ScheduledExecutorService mainLoopExecutor = Executors.newSingleThreadScheduledExecutor();
    private InetAddress joiningPeer;
    private int mainLoopInterval = 100;

    public DelaygroupingOrchestrator(DelaygroupingConfiguration config, BiConsumer<MqttPublishMessage, String> internalPublishFunction) {
        this.internalPublishFunction = internalPublishFunction;
        cloudAnchor = config.getAnchorNodeAddress();
        latencyThreshold = config.getLatencyThreshold();
        this.localInterfaceAddress = config.getHost();
        clientId = localInterfaceAddress.getHostAddress();
        this.connectionMonitor = new ConnectionMonitor(1000, 20);
        peerConnectionManager = new PeerConnectionManager(localInterfaceAddress, this::handleNewPeerConnection);
        leader = null;
        joiningPeer = null;

        state = OrchestratorState.BOOTSTRAP;

        // Let's get this party started!
        doNext(this::transitionToLeader);

    }

    private void doNext(Runnable runnable) {
        doNext(runnable, 0);
    }

    private void doNext(Runnable runnable, int additionalDelay) {
        mainLoopExecutor.schedule(runnable, mainLoopInterval + additionalDelay, TimeUnit.MILLISECONDS);
    }

    private void doImmediately(Runnable runnable) {
        mainLoopExecutor.execute(runnable);
    }

    private void transitionToLeader() {
        LOG.info("{}: Switching to leader mode", clientId);

        if (leader != null) {
            LOG.info("Leaving existing group with leader {}", leader.getHostName());
            sendMessageToLeader(PeerMessageMembership.leave(localInterfaceAddress));
        }
        leader = null;

        anchorConnection = new AnchorConnection(cloudAnchor, localInterfaceAddress);
        anchorConnection.startLeaderAnnouncement();
        anchorConnection.setMqttCallback(this::handleAnchorPublishMessage);
        clientSubscriptions.getFlattened().forEach(topicFilter -> anchorConnection.addSubscription(topicFilter));

        previousLeaders = new HashSet<>();

        state = OrchestratorState.LEADER;

        doNext(this::leaderAction);
    }

    private void leaderAction() {
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

        if (minimumDelayLeader != null && !isJoinOngoing()) {
            LOG.info("Found other leader {} with delay {}ms", minimumDelayLeader.getHostName(), minimumDelay);
            // if we have a candidate start negotiation (see handlers)
            peerConnectionManager.getConnectionToPeer(minimumDelayLeader)
                .sendMessage(PeerMessageMembership.join(leaderCapabilityMeasure));
            doNext(this::leaderAction, new Random().nextInt(1000));
        } else {
            doNext(this::leaderAction);
        }
    }

    private void transitionToNonLeader(InetAddress newLeader) {
        LOG.info("{}: Switching to non-leader mode and connecting to new leader {}", clientId, newLeader);

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
        LOG.info("Sending group subscriptions to leader: {}", subscriptions);
        sendMessageToLeader(PeerMessageSubscribe.fromTopicFilter(subscriptions));
        sendMessageToLeader(PeerMessageMembership.groupUpdate(null, new ArrayList<>(groupMembers)));

        // point group members to new leader
        sendMessageToGroup(PeerMessageRedirect.redirect(newLeader));
        groupMembers.clear();
        groupSubscriptions.clear();

        doNext(this::nonLeaderAction);
    }

    private void nonLeaderAction() {
        // monitor latency to group leader and switch to leader (effectively leaving group)
        Double leaderDelay;
        try {
            leaderDelay = connectionMonitor.getAverageDelay(leader).get();
            if (leaderDelay > latencyThreshold) {
                LOG.info("Our leader exceed the latency threshold ({} > {})", leaderDelay, latencyThreshold);
                doImmediately(this::transitionToLeader);
            }
        } catch (InterruptedException | ExecutionException ignored) {
        }

        doNext(this::nonLeaderAction);
    }

    private void handleMembershipMessages(PeerMessageMembership msg, PeerConnection origin) {
        // TODO This is a bit iffy because we may get duplicated messages from legitimately joining peer... ==> refactor this into a proper membership transaction tracking object
        if (joiningPeer != null && joiningPeer != origin.getRemoteAddress()) {
            LOG.info("Peer {} is already joining! Try later...", joiningPeer.getHostAddress());
            origin.sendMessage(PeerMessageMembership.busy());
            return;
        }
        switch (msg.getSignal()) {
            case JOIN:
                // Only allow join if we are a leader
                if (state.equals(OrchestratorState.LEADER)) {
                    if (leaderCapabilityMeasure >= msg.getElectionValue()) {
                        LOG.info("Got JOIN request. New peer {} will join my group as member.", origin.getRemoteAddress().getHostName());
                        origin.sendMessage(PeerMessageMembership.joinAck(false));
                        origin.sendMessage(PeerMessageMembership.groupUpdate(null, new ArrayList<>(groupMembers)));
                    } else {
                        LOG.info("Got JOIN request. New peer {} will be the leader.", origin.getRemoteAddress().getHostName());
                        origin.sendMessage(PeerMessageMembership.joinAck(true));
                    }
                    joiningPeer = origin.getRemoteAddress();
                } else {
                    LOG.info("Got JOIN request from {} but I'm not leader. Denying...", origin.getRemoteAddress().getHostName());
                    origin.sendMessage(PeerMessageMembership.deny());
                }
                break;
            case JOIN_ACK:
                // We are allowed to join!
                if (msg.isShouldBeLeader()) {
                    if (state.equals(OrchestratorState.NON_LEADER)) {
                        doImmediately(this::transitionToLeader); // this should theoretically never be the case
                    }
                    LOG.info("Got JOIN_ACK from {}. I should be leader!", origin.getRemoteAddress().getHostName());
                    origin.sendMessage(PeerMessageMembership.joinAckAck(msg));
                } else {
                    if (state.equals(OrchestratorState.LEADER)) {
                        // we join the other group and should not be leader
                        doImmediately(() -> transitionToNonLeader(origin.getRemoteAddress())); // this should theoretically always be the case (except for after we've implemented full JOIN on migrate)
                    }
                    LOG.info("Got JOIN_ACK from {}. I've stepped down to NON_LEADER. She will take over.", origin.getRemoteAddress().getHostName());
                    origin.sendMessage(PeerMessageMembership.joinAckAck(msg));
                }
                break;
            case JOIN_ACKACK:
                if (state.equals(OrchestratorState.LEADER)) {
                    if (msg.isShouldBeLeader()) {
                        LOG.info("Got JOIN_ACKACK from {}. She joins as group member, so just add her.", origin.getRemoteAddress().getHostName());
                        sendMessageToGroup(PeerMessageMembership.groupUpdate(null, Arrays.asList(origin.getRemoteAddress())));
                        groupMembers.add(origin.getRemoteAddress());
                        LOG.info("Group members after JOIN: {}", groupMembers);
                    } else {
                        LOG.info("Got JOIN_ACKACK from {}. She joins as leader and confirmed switching.", origin.getRemoteAddress().getHostName());
                        doImmediately(() -> transitionToNonLeader(origin.getRemoteAddress()));
                    }
                    joiningPeer = null;
                } else {
                    LOG.warn("Got JOIN_ACKACK from {}. Ignoring it as I'm NOT LEADER!", origin.getRemoteAddress().getHostName());
                }
                // ignore JOIN_ACKACK if we're not leader
                break;
            case BUSY:
                LOG.info("Got BUSY from {}. Scheduling to retry JOIN after random time.", origin.getRemoteAddress().getHostName());
                peerMessagingExecutor.schedule(
                    () -> origin.sendMessage(PeerMessageMembership.join(leaderCapabilityMeasure)),
                    new Random().nextInt(1000), TimeUnit.MILLISECONDS);
                break;
            case DENY:
                LOG.info("Got DENY from {}.", origin.getRemoteAddress().getHostName());
                break;
            case LEAVE:
                if (state.equals(OrchestratorState.LEADER)) {
                    LOG.info("Got LEAVE from {}. Removing her from group.", origin.getRemoteAddress().getHostName());
                    peerConnectionManager.closeConnectionToPeer(msg.getLeavingPeer());
                    groupMembers.remove(msg.getLeavingPeer());
                    sendMessageToGroup(PeerMessageMembership.groupUpdate(Arrays.asList(msg.getLeavingPeer()), null));
                    LOG.info("Group members after LEAVE: {}", groupMembers);
                } else {
                    LOG.warn("Got LEAVE from {}. Ignoring it, because I'm NOT LEADER!", origin.getRemoteAddress().getHostName());
                }
                break;
            case GROUP_UPDATE:
                if (state.equals(OrchestratorState.NON_LEADER)) {
                    LOG.info("Got GROUP_UPDATE from {}. Updating group member info (I'm NON_LEADER).", origin.getRemoteAddress().getHostName());
                } else if (state.equals(OrchestratorState.LEADER)) {
                    LOG.info("Got GROUP_UPDATE from {}. Updating group member info (I'm LEADER).", origin.getRemoteAddress().getHostName());
                }
                groupMembers.removeAll(msg.getLeftPeers());
                groupMembers.addAll(msg.getJoinedPeers());
                LOG.info("Group members after GROUP_UPDATE: {}", groupMembers);
                break;
        }
    }

    private void handleRedirectMessages(PeerMessageRedirect msg, PeerConnection origin) {
        if (state.equals(OrchestratorState.NON_LEADER)) {
            LOG.info("Got REDIRECT from {}. Migrating to new leader {}", origin.getRemoteAddress().getHostName(), msg.getTarget());
            peerConnectionManager.closeConnectionToPeer(leader);
            connectionMonitor.removeAll();
            leader = msg.getTarget();
            peerConnectionManager.getConnectionToPeer(leader);
            connectionMonitor.addMonitoredPeer(leader);
            // Lets not do the whole JOIN thing for now and just trust in the new leader having received all its leader state (although it would probably be more correct, i.e. safer)
        } else {
            LOG.warn("Got REDIRECT from {}. Ignoring it as I'm LEADER!", origin.getRemoteAddress().getHostName());
        }
    }

    private void handlePublishMessages(PeerMessagePublish msg, PeerConnection origin) {
        LOG.debug("Got PUBLISH from {}: {}", origin.getRemoteAddress().getHostName(), msg.getPublishMessages());
        msg.getPublishMessages().forEach(this::internalPublish);
        if (state.equals(OrchestratorState.LEADER)) {
            LOG.debug("Relaying PUBLISH to anchor");
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
        LOG.info("Intercepted PUBLISH from clients: {}", interceptedMsg);
        if (state.equals(OrchestratorState.LEADER)) {
            anchorConnection.publish(interceptedMsg);
        }
        if (groupSubscriptions.matches(interceptedMsg.variableHeader().topicName())) {
            sendMessageToGroup(PeerMessagePublish.fromMessage(interceptedMsg));
        }
    }

    private void handleInterceptedSubscribe(String topicFilter) {
        LOG.info("Intercepted SUBSCRIBE from clients: ", topicFilter);
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
        LOG.info("Got MQTT PUBLISH from anchor: {}", msg);
        var mqttPubMsg = Utils.mqttPublishMessageFromValues(msg.topic, msg.payload);
        internalPublish(mqttPubMsg);
        if (groupSubscriptions.matches(msg.topic)) {
            sendMessageToGroup(PeerMessagePublish.fromMessage(mqttPubMsg));
        }
    }

    private void sendMessageToLeader(PeerMessage msg) {
        if (state.equals(OrchestratorState.NON_LEADER) && leader != null) {
            LOG.info("Sending message {} to leader {}", msg.type, leader.getHostName());
            peerConnectionManager.getConnectionToPeer(leader).sendMessage(msg);
        } else {
            LOG.error("Error while trying to send message to leader! Leader not known or not in NON_LEADER state. Message: {}", msg);
        }
    }

    private void sendMessageToGroup(PeerMessage msg) {
        LOG.info("Sending message {} to group: {}", msg.type, groupMembers);
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

    public void shutdown() {
        mainLoopExecutor.shutdown();
        try {
            mainLoopExecutor.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            mainLoopExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        peerMessagingExecutor.shutdown();
        try {
            peerMessagingExecutor.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            peerMessagingExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        if (peerConnectionManager != null) {
            peerConnectionManager.shutdown();
        }
        if (connectionMonitor != null) {
            connectionMonitor.shutdown();
        }
        if (anchorConnection != null) {
            anchorConnection.shutdown();
        }
    }

    private boolean isJoinOngoing() {
        return joiningPeer != null;
    }

    public Consumer<MqttPublishMessage> getInterceptHandler() {
        return this::handleInterceptedPublish;
    }

    public Consumer<String> getSubscribeHandler() {
        return this::handleInterceptedSubscribe;
    }

    private enum OrchestratorState {
        LEADER,
        NON_LEADER,
        BOOTSTRAP,
    }
}
