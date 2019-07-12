package io.moquette.delaygrouping;

import io.moquette.delaygrouping.anchor.AnchorConnection;
import io.moquette.delaygrouping.anchor.Message;
import io.moquette.delaygrouping.monitoring.ConnectionMonitor;
import io.moquette.delaygrouping.peering.PeerConnection;
import io.moquette.delaygrouping.peering.PeerConnectionManager;
import io.moquette.delaygrouping.peering.messaging.*;
import io.moquette.logging.MessageLogger;
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
    private InetAddress bindAddress;
    private int latencyThreshold;
    private AnchorConnection anchorConnection;
    private String clientId;
    private BiConsumer<MqttPublishMessage, String> internalPublishFunction;
    private OrchestratorState state;
    private ConnectionMonitor connectionMonitor;
    private Set<InetAddress> previousLeaders;
    private InetAddress leader;
    private PeerConnectionManager peerConnectionManager;
    private int leadershipCapabilityMeasure;
    private Set<InetAddress> groupMembers = ConcurrentHashMap.newKeySet();
    // TODO Check when to reset group / client subscriptions?!
    private SubscriptionStore groupSubscriptions = new SubscriptionStore();
    private ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    private MembershipTransaction membershipTransaction;
    private int mainLoopInterval = 100;
    private MessageLogger messageLogger;
    private InetAddress name;

    public DelaygroupingOrchestrator(DelaygroupingConfiguration config, BiConsumer<MqttPublishMessage, String> internalPublishFunction) {
        this.internalPublishFunction = internalPublishFunction;
        this.leadershipCapabilityMeasure = config.getLeadershipCapabilityMeasure();
        cloudAnchor = config.getAnchorNodeAddress();
        latencyThreshold = config.getLatencyThreshold();
        this.name = config.getName();
        this.bindAddress = config.getBindHost();
        clientId = config.getClientId();
        this.connectionMonitor = new ConnectionMonitor(1000, 20);
        peerConnectionManager = new PeerConnectionManager(bindAddress, this::handleNewPeerConnection);
        leader = null;
        messageLogger = new MessageLogger(clientId);

        state = OrchestratorState.BOOTSTRAP;

        // Let's get this broker started!
        doNext(this::transitionToLeader);
    }

    private void doNext(Runnable runnable) {
        doNext(runnable, 0);
    }

    private void doNext(Runnable runnable, int additionalDelay) {
        executor.schedule(runnable, mainLoopInterval + additionalDelay, TimeUnit.MILLISECONDS);
    }

    private void doImmediately(Runnable runnable) {
        executor.execute(runnable);
    }

    private void transitionToLeader() {
        LOG.info("{}: Switching to leader mode", clientId);
        state = OrchestratorState.LEADER;

        if (leader != null) {
            LOG.info("Leaving existing group with leader {}", leader.getHostAddress());
            sendMessageToLeader(PeerMessageMembership.leave(name));
            groupMembers.clear();
        }
        leader = null;

        anchorConnection = new AnchorConnection(cloudAnchor, name, clientId);
        anchorConnection.startLeaderAnnouncement();
        anchorConnection.setMqttCallback(this::handleAnchorPublishMessage);
        groupSubscriptions.getFlattened().forEach(topicFilter -> anchorConnection.addSubscription(topicFilter));

        previousLeaders = new HashSet<>();


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
        for (InetAddress otherLeader : currentLeaders) {
            try {
                var averageDelay = connectionMonitor.getAverageDelay(otherLeader).get();
                if (averageDelay < latencyThreshold && averageDelay < minimumDelay) {
                    minimumDelay = averageDelay;
                    minimumDelayLeader = otherLeader;
                }
            } catch (InterruptedException | ExecutionException e) {
                LOG.error("Got exception while getting conn stats: {}", e.getCause());
            }
        }

        if (minimumDelayLeader != null && !isMembershipTransactionPending()) {
            LOG.info("Found other leader {} with delay {}ms", minimumDelayLeader.getHostAddress(), minimumDelay);
            // if we have a candidate start negotiation (see handlers)
            var joinMessage = PeerMessageMembership.join(leadershipCapabilityMeasure);
            membershipTransaction = new MembershipTransaction(minimumDelayLeader);
            membershipTransaction.commitMessage(joinMessage);
            peerConnectionManager.getConnectionToPeer(minimumDelayLeader)
                .sendMessage(joinMessage);
            doNext(this::leaderAction, new Random().nextInt(1000));
        } else {
            doNext(this::leaderAction);
        }
    }

    private void transitionToNonLeader(InetAddress newLeader) {
        LOG.info("{}: Switching to non-leader mode and connecting to new leader {}", clientId, newLeader.getHostAddress());
        state = OrchestratorState.NON_LEADER;

        // stop all leader-related functions
        connectionMonitor.removeAll();
        anchorConnection.shutdown();
        anchorConnection = null;

        // connect to new leader
        leader = newLeader;
        connectionMonitor.addMonitoredPeer(newLeader);

        // point existing group members to new leader
        sendMessageToGroup(PeerMessageRedirect.redirect(newLeader));

        doNext(this::nonLeaderAction);
    }

    private void nonLeaderAction() {
        // monitor latency to group leader and switch to leader (effectively leaving group)
        Double leaderDelay;
        try {
            leaderDelay = connectionMonitor.getAverageDelay(leader).get();
            //LOG.debug("Leader delay: {}ms", leaderDelay);
            if (leaderDelay > latencyThreshold) {
                LOG.debug("Our leader exceed the latency threshold ({}ms > {}ms)", leaderDelay, latencyThreshold);
                doImmediately(this::transitionToLeader);
            }
        } catch (InterruptedException | ExecutionException ignored) {
        }

        doNext(this::nonLeaderAction);
    }

    private void handleMembershipMessage(PeerMessageMembership msg, PeerConnection origin) {

        // TODO This has a problem if two clients shoot CONNECTs at each other at the same time (both don't allow JOIN because they've already sent JOIN) --> maybe evaluate JOIN message contents and give in if non leader
        // Handle membership transactions
        if (membershipTransaction != null) {
            if (membershipTransaction.hasExpired()) {
                membershipTransaction = new MembershipTransaction(origin.getRemoteAddress());
            } else {
                // okay, we still have to obey this one
                if (!membershipTransaction.getPeerAddress().equals(origin.getRemoteAddress())) {
                    LOG.debug("Peer {} is already joining! Try later...", membershipTransaction.getPeerAddress());
                    return;
                }
            }
        } else {
            membershipTransaction = new MembershipTransaction(origin.getRemoteAddress());
        }
        if (!membershipTransaction.commitMessage(msg)) {
            LOG.info("Illegal state transition attempted by {}: {} -> {}. Ignoring...", origin.getRemoteAddress().getHostAddress(), membershipTransaction.getLastSignal(), msg.getSignal());
            return;
        }

        // If we've made it until here, everything is good!

        switch (msg.getSignal()) {
            case JOIN:
                // Only allow join if we are a leader
                if (state.equals(OrchestratorState.LEADER)) {
                    if (leadershipCapabilityMeasure >= msg.getElectionValue()) {
                        LOG.debug("Got JOIN request. New peer {} will join my group as member.", origin.getRemoteAddress().getHostAddress());
                        var joinAckMsg = PeerMessageMembership.joinAck(false, groupMembers, groupSubscriptions);
                        membershipTransaction.commitMessage(joinAckMsg);
                        origin.sendMessage(joinAckMsg);
                    } else {
                        LOG.debug("Got JOIN request. New peer {} will be the leader.", origin.getRemoteAddress().getHostAddress());
                        var joinAckMsg = PeerMessageMembership.joinAck(true, groupMembers, groupSubscriptions);
                        membershipTransaction.commitMessage(joinAckMsg);
                        origin.sendMessage(joinAckMsg);
                    }
                } else {
                    LOG.info("Got JOIN request from {} but I'm not leader. Ignoring...", origin.getRemoteAddress().getHostAddress());
                    // TODO We could invalidate the membership transaction here...
                }
                break;
            case JOIN_ACK:
                // We are allowed to join!
                if (state.equals(OrchestratorState.LEADER)) {
                    if (msg.isShouldBeLeader()) {
                        LOG.info("Got JOIN_ACK from {}. I should be leader! Adding peer to my group...", origin.getRemoteAddress().getHostAddress());
                        groupMembers.add(origin.getRemoteAddress());
                        groupMembers.addAll(msg.getGroupMembers());
                        groupSubscriptions.merge(msg.getGroupSubscriptions());
                        var joinAckAckMsg = PeerMessageMembership.joinAckAck(msg, groupMembers, groupSubscriptions);
                        membershipTransaction.commitMessage(joinAckAckMsg);
                        origin.sendMessage(joinAckAckMsg);
                        sendMessageToGroup(PeerMessageGroupUpdate.update(groupMembers, groupSubscriptions));
                        LOG.info("Group members after JOIN: {}", groupMembers);
                    } else {
                        LOG.info("Got JOIN_ACK from {}. I'm stepping down to NON_LEADER. She will take over.", origin.getRemoteAddress().getHostAddress());
                        transitionToNonLeader(origin.getRemoteAddress());
                        var joinAckAckMsg = PeerMessageMembership.joinAckAck(msg, groupMembers, groupSubscriptions);
                        membershipTransaction.commitMessage(joinAckAckMsg);
                        origin.sendMessage(joinAckAckMsg);
                        // important: Do this after sending our state back
                        groupMembers.addAll(msg.getGroupMembers());
                        groupSubscriptions.merge(msg.getGroupSubscriptions());
                    }
                } else {
                    LOG.info("Got JOIN_ACK from {} but I'm not leader. Ignoring...", origin.getRemoteAddress().getHostAddress());
                }
                break;
            case JOIN_ACKACK:
                if (state.equals(OrchestratorState.LEADER)) {
                    if (msg.isShouldBeLeader()) {
                        LOG.info("Got JOIN_ACKACK from {}. She joins as group member, so add her and save her state.", origin.getRemoteAddress().getHostAddress());
                        groupMembers.add(origin.getRemoteAddress());
                        groupMembers.addAll(msg.getGroupMembers());
                        groupSubscriptions.merge(msg.getGroupSubscriptions());
                        sendMessageToGroup(PeerMessageGroupUpdate.update(groupMembers, groupSubscriptions));
                        LOG.info("Group members after JOIN: {}", groupMembers);
                    } else {
                        LOG.info("Got JOIN_ACKACK from {}. She joins as leader and confirmed switching / being ready.", origin.getRemoteAddress().getHostAddress());
                        groupMembers.addAll(msg.getGroupMembers());
                        groupSubscriptions.merge(msg.getGroupSubscriptions());
                        transitionToNonLeader(origin.getRemoteAddress());
                    }
                } else {
                    LOG.warn("Got JOIN_ACKACK from {}. Ignoring it as I'm NOT LEADER!", origin.getRemoteAddress().getHostAddress());
                }
                // ignore JOIN_ACKACK if we're not leader
                break;
            case LEAVE:
                if (state.equals(OrchestratorState.LEADER)) {
                    LOG.debug("Got LEAVE from {}. Removing her from group.", origin.getRemoteAddress().getHostAddress());
                    peerConnectionManager.closeConnectionToPeer(msg.getLeavingPeer());
                    groupMembers.remove(msg.getLeavingPeer());
                    sendMessageToGroup(PeerMessageGroupUpdate.update(groupMembers, new SubscriptionStore()));
                    LOG.debug("Group members after LEAVE: {}", groupMembers);
                } else {
                    LOG.warn("Got LEAVE from {}. Ignoring it, because I'm NOT LEADER!", origin.getRemoteAddress().getHostAddress());
                }
                break;
        }
    }

    private void handleGroupMessage(PeerMessageGroupUpdate msg, PeerConnection origin) {
        if (state.equals(OrchestratorState.NON_LEADER) && !isMembershipTransactionPending()) {
            groupMembers.addAll(msg.getGroupMembers());
            groupMembers.retainAll(msg.getGroupMembers());
            groupSubscriptions.merge(msg.getGroupSubscriptions());
            LOG.info("Got group update from {}. Updated group: {}", origin.getRemoteAddress().getHostAddress(), state.name(), groupMembers);
        } else {
            LOG.info("Ignoring group update from {}.!", origin.getRemoteAddress().getHostAddress());
        }
    }

    private void handleRedirectMessage(PeerMessageRedirect msg, PeerConnection origin) {
        if (msg.getTarget().equals(leader)) {
            LOG.info("Got REDIRECT from {}. Ignoring it because I'm already connected to her...", origin.getRemoteAddress().getHostAddress());
            return;
        }
        if (state.equals(OrchestratorState.NON_LEADER)) {
            LOG.info("Got REDIRECT from {}. Migrating to new leader {}", origin.getRemoteAddress().getHostAddress(), msg.getTarget());
            peerConnectionManager.closeConnectionToPeer(leader);
            connectionMonitor.removeAll();
            leader = msg.getTarget();
            peerConnectionManager.getConnectionToPeer(leader);
            connectionMonitor.addMonitoredPeer(leader);
            // Lets not do the whole JOIN thing for now and just trust in the new leader having received all its leader state (although it would probably be more correct, i.e. safer)
        } else {
            LOG.warn("Got REDIRECT from {}. Ignoring it as I'm LEADER!", origin.getRemoteAddress().getHostAddress());
        }
    }

    private void handlePublishMessage(PeerMessagePublish msg, PeerConnection origin) {
        messageLogger.log(msg);
        LOG.debug("Got PUBLISH from {}: {}", origin.getRemoteAddress().getHostAddress(), msg.getPublishMessages());
        msg.getPublishMessages().forEach(this::internalPublish);
        if (state.equals(OrchestratorState.LEADER)) {
            LOG.debug("Relaying PUBLISH to anchor");
            anchorConnection.publish(msg);
        }
    }

    private void handleSubscribeMessage(PeerMessageSubscribe msg, PeerConnection origin) {
        LOG.info("Got SUBSCRIBE from {}: {}", origin.getRemoteAddress().getHostAddress(), msg.getTopicFilters());
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
                handlePublishMessage((PeerMessagePublish) msg, origin);
                break;
            case REDIRECT:
                doImmediately(() -> handleRedirectMessage((PeerMessageRedirect) msg, origin));
                break;
            case SUBSCRIBE:
                handleSubscribeMessage((PeerMessageSubscribe) msg, origin);
                break;
            case MEMBERSHIP:
                doImmediately(() -> handleMembershipMessage((PeerMessageMembership) msg, origin));
                break;
            case GROUP:
                doImmediately(() -> handleGroupMessage((PeerMessageGroupUpdate) msg, origin));
                break;
        }
    }

    private void handleNewPeerConnection(PeerConnection connection) {
        // this is run synchronously in channel initializer, so don't do anything costly here!
        connection.registerMessageHandler(this::handlePeerMessage,
            PeerMessageType.SUBSCRIBE, PeerMessageType.PUBLISH, PeerMessageType.REDIRECT, PeerMessageType.MEMBERSHIP);
    }

    private void handleInterceptedPublish(MqttPublishMessage interceptedMsg) {
        messageLogger.log(interceptedMsg);
        LOG.debug("Intercepted PUBLISH from clients: {}", interceptedMsg);
        var peerPubMsg = PeerMessagePublish.fromMessage(interceptedMsg);
        if (state.equals(OrchestratorState.LEADER)) {
            anchorConnection.publish(interceptedMsg);
        } else {
            sendMessageToLeader(peerPubMsg);
        }
        if (groupSubscriptions.matches(interceptedMsg.variableHeader().topicName())) {
            sendMessageToGroup(peerPubMsg);
        }
    }

    private void handleInterceptedSubscribe(String topicFilter) {
        LOG.info("Intercepted SUBSCRIBE from clients: {}", topicFilter);
        // We need to keep track of our clients subscriptions for group migration (to send our subscriptions to the new leader)
        groupSubscriptions.addSubscription(clientId, topicFilter);

        if (state.equals(OrchestratorState.NON_LEADER)) {
            // Only send subscribe to leader as it will get relayed by the leader
            sendMessageToLeader(PeerMessageSubscribe.fromTopicFilter(Arrays.asList(topicFilter)));
        } else if (state.equals(OrchestratorState.LEADER)) {
            anchorConnection.addSubscription(topicFilter);
            sendMessageToGroup(PeerMessageSubscribe.fromTopicFilter(Arrays.asList(topicFilter)));
        }
    }

    private void handleAnchorPublishMessage(Message msg) {
        messageLogger.log(msg.topic, msg.payload);
        LOG.debug("Got MQTT PUBLISH from anchor: {}", msg);
        var mqttPubMsg = Utils.mqttPublishMessageFromValues(msg.topic, msg.payload);
        internalPublish(mqttPubMsg);
        if (groupSubscriptions.matches(msg.topic)) {
            sendMessageToGroup(PeerMessagePublish.fromMessage(mqttPubMsg));
        }
    }

    private void sendMessageToLeader(PeerMessage msg) {
        if (leader != null) {
            LOG.debug("Sending message {} to leader {}", msg.type, leader.getHostAddress());
            peerConnectionManager.getConnectionToPeer(leader).sendMessage(msg);
        } else {
            LOG.error("Error while trying to send message to leader! Leader not known or not in NON_LEADER state. Message: {}", msg);
        }
    }

    private void sendMessageToGroup(PeerMessage msg) {
        LOG.debug("Sending message {} to group: {}", msg.type, groupMembers);
        groupMembers.forEach(member -> {
            // Don't send to ourselves
            if (!member.equals(name)) {
                peerConnectionManager.getConnectionToPeer(member).sendMessage(msg);
            }
        });
    }

    private void internalPublish(MqttPublishMessage msg) {
        internalPublishFunction.accept(msg, clientId);
    }

    public void shutdown() {
        executor.shutdown();
        try {
            executor.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            executor.shutdownNow();
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

    private boolean isMembershipTransactionPending() {
        return membershipTransaction != null && !membershipTransaction.hasExpired();
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
