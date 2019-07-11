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
    private SubscriptionStore clientSubscriptions = new SubscriptionStore(); // TODO We never unsubscribe...
    private ScheduledExecutorService peerMessagingExecutor = Executors.newSingleThreadScheduledExecutor();
    private ScheduledExecutorService mainLoopExecutor = Executors.newSingleThreadScheduledExecutor();
    private InetAddress joiningPeer;
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
        joiningPeer = null;
        messageLogger = new MessageLogger(clientId);

        state = OrchestratorState.BOOTSTRAP;

        // Let's get this broker started!
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
            LOG.info("Leaving existing group with leader {}", leader.getHostAddress());
            sendMessageToLeader(PeerMessageMembership.leave(name));
            groupMembers.clear();
        }
        leader = null;

        anchorConnection = new AnchorConnection(cloudAnchor, name, clientId);
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

        if (minimumDelayLeader != null && !isJoinOngoing()) {
            LOG.info("Found other leader {} with delay {}ms", minimumDelayLeader.getHostAddress(), minimumDelay);
            // if we have a candidate start negotiation (see handlers)
            peerConnectionManager.getConnectionToPeer(minimumDelayLeader)
                .sendMessage(PeerMessageMembership.join(leadershipCapabilityMeasure));
            doNext(this::leaderAction, new Random().nextInt(1000));
        } else {
            doNext(this::leaderAction);
        }
    }

    private void transitionToNonLeader(InetAddress newLeader) {
        LOG.info("{}: Switching to non-leader mode and connecting to new leader {}", clientId, newLeader.getHostAddress());

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
        LOG.debug("Sending group subscriptions to leader: {}", subscriptions);
        sendMessageToLeader(PeerMessageSubscribe.fromTopicFilter(subscriptions));
        sendMessageToLeader(PeerMessageMembership.groupUpdate(new ArrayList<>(groupMembers), null));

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
            //LOG.debug("Leader delay: {}ms", leaderDelay);
            if (leaderDelay > latencyThreshold) {
                LOG.debug("Our leader exceed the latency threshold ({}ms > {}ms)", leaderDelay, latencyThreshold);
                doImmediately(this::transitionToLeader);
            }
        } catch (InterruptedException | ExecutionException ignored) {
        }

        doNext(this::nonLeaderAction);
    }

    private void handleMembershipMessages(PeerMessageMembership msg, PeerConnection origin) {
        // TODO This is a bit iffy because we may get duplicated messages from legitimately joining peer... ==> refactor this into a proper membership transaction tracking object
        if (joiningPeer != null && joiningPeer != origin.getRemoteAddress()) {
            LOG.debug("Peer {} is already joining! Try later...", joiningPeer.getHostAddress());
            origin.sendMessage(PeerMessageMembership.busy());
            return;
        }
        switch (msg.getSignal()) {
            case JOIN:
                // Only allow join if we are a leader
                if (state.equals(OrchestratorState.LEADER)) {
                    if (leadershipCapabilityMeasure >= msg.getElectionValue()) {
                        LOG.debug("Got JOIN request. New peer {} will join my group as member.", origin.getRemoteAddress().getHostAddress());
                        origin.sendMessage(PeerMessageMembership.joinAck(false));
                        origin.sendMessage(PeerMessageMembership.groupUpdate(new ArrayList<>(groupMembers), null));
                    } else {
                        LOG.debug("Got JOIN request. New peer {} will be the leader.", origin.getRemoteAddress().getHostAddress());
                        origin.sendMessage(PeerMessageMembership.joinAck(true));
                    }
                    joiningPeer = origin.getRemoteAddress();
                } else {
                    LOG.debug("Got JOIN request from {} but I'm not leader. Denying...", origin.getRemoteAddress().getHostAddress());
                    origin.sendMessage(PeerMessageMembership.deny());
                }
                break;
            case JOIN_ACK:
                // We are allowed to join!
                if (msg.isShouldBeLeader()) {
                    if (state.equals(OrchestratorState.NON_LEADER)) {
                        doImmediately(this::transitionToLeader); // this should theoretically never be the case
                    }
                    LOG.debug("Got JOIN_ACK from {}. I should be leader! Adding peer to my group...", origin.getRemoteAddress().getHostAddress());
                    groupMembers.add(origin.getRemoteAddress());
                    origin.sendMessage(PeerMessageMembership.joinAckAck(msg));
                    LOG.info("Group members after JOIN: {}", groupMembers);
                } else {
                    if (state.equals(OrchestratorState.LEADER)) {
                        // we join the other group and should not be leader
                        doImmediately(() -> transitionToNonLeader(origin.getRemoteAddress())); // this should theoretically always be the case (except for after we've implemented full JOIN on migrate)
                    }
                    LOG.debug("Got JOIN_ACK from {}. I've stepped down to NON_LEADER. She will take over.", origin.getRemoteAddress().getHostAddress());
                    origin.sendMessage(PeerMessageMembership.joinAckAck(msg));
                }
                break;
            case JOIN_ACKACK:
                if (state.equals(OrchestratorState.LEADER)) {
                    if (msg.isShouldBeLeader()) {
                        LOG.debug("Got JOIN_ACKACK from {}. She joins as group member, so just add her.", origin.getRemoteAddress().getHostAddress());
                        groupMembers.add(origin.getRemoteAddress());
                        sendMessageToGroup(PeerMessageMembership.groupSet(groupMembers));
                        LOG.info("Group members after JOIN: {}", groupMembers);
                    } else {
                        LOG.debug("Got JOIN_ACKACK from {}. She joins as leader and confirmed switching / being ready.", origin.getRemoteAddress().getHostAddress());
                        doImmediately(() -> transitionToNonLeader(origin.getRemoteAddress()));
                    }
                    joiningPeer = null;
                } else {
                    LOG.warn("Got JOIN_ACKACK from {}. Ignoring it as I'm NOT LEADER!", origin.getRemoteAddress().getHostAddress());
                }
                // ignore JOIN_ACKACK if we're not leader
                break;
            case BUSY:
                LOG.debug("Got BUSY from {}.", origin.getRemoteAddress().getHostAddress());
                break;
            case DENY:
                LOG.debug("Got DENY from {}.", origin.getRemoteAddress().getHostAddress());
                break;
            case LEAVE:
                if (state.equals(OrchestratorState.LEADER)) {
                    LOG.debug("Got LEAVE from {}. Removing her from group.", origin.getRemoteAddress().getHostAddress());
                    peerConnectionManager.closeConnectionToPeer(msg.getLeavingPeer());
                    groupMembers.remove(msg.getLeavingPeer());
                    sendMessageToGroup(PeerMessageMembership.groupUpdate(null, Arrays.asList(msg.getLeavingPeer())));
                    LOG.debug("Group members after LEAVE: {}", groupMembers);
                } else {
                    LOG.warn("Got LEAVE from {}. Ignoring it, because I'm NOT LEADER!", origin.getRemoteAddress().getHostAddress());
                }
                break;
            case GROUP_UPDATE:
                groupMembers.removeAll(msg.getRemovedPeers());
                groupMembers.addAll(msg.getAddedPeers());
                LOG.info("Got GROUP_UPDATE from {} (I'm {}). Updated group: {}", origin.getRemoteAddress().getHostAddress(), state.name(), groupMembers);

                if (state.equals(OrchestratorState.LEADER)) {
                    sendMessageToGroup(PeerMessageMembership.groupSet(groupMembers));
                }
                break;
            case GROUP_SET:
                groupMembers.addAll(msg.getGroupMembers());
                groupMembers.retainAll(msg.getGroupMembers());
                LOG.info("Got GROUP_SET from {}. Updated group members: {}", origin.getRemoteAddress().getHostAddress(), groupMembers);
                break;
        }
    }

    private void handleRedirectMessages(PeerMessageRedirect msg, PeerConnection origin) {
        if (!msg.getTarget().equals(name)) {
            LOG.info("Got REDIRECT from {}. Ignoring it because I'm already connected to her...", origin.getRemoteAddress().getHostAddress());
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

    private void handlePublishMessages(PeerMessagePublish msg, PeerConnection origin) {
        messageLogger.log(msg);
        LOG.debug("Got PUBLISH from {}: {}", origin.getRemoteAddress().getHostAddress(), msg.getPublishMessages());
        msg.getPublishMessages().forEach(this::internalPublish);
        if (state.equals(OrchestratorState.LEADER)) {
            LOG.debug("Relaying PUBLISH to anchor");
            anchorConnection.publish(msg);
        }
    }

    private void handleSubscribeMessages(PeerMessageSubscribe msg, PeerConnection origin) {
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
                handlePublishMessages((PeerMessagePublish) msg, origin);
                break;
            case REDIRECT:
                peerMessagingExecutor.execute(() -> handleRedirectMessages((PeerMessageRedirect) msg, origin));
                break;
            case SUBSCRIBE:
                handleSubscribeMessages((PeerMessageSubscribe) msg, origin);
                break;
            case MEMBERSHIP:
                peerMessagingExecutor.execute(() -> handleMembershipMessages((PeerMessageMembership) msg, origin));
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
        clientSubscriptions.addSubscription(clientId, topicFilter);

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
        if (state.equals(OrchestratorState.NON_LEADER) && leader != null) {
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
