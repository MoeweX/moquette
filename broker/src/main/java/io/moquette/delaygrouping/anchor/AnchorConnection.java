package io.moquette.delaygrouping.anchor;

import io.moquette.delaygrouping.Utils;
import io.moquette.delaygrouping.peering.messaging.PeerMessagePublish;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

public class AnchorConnection {
    private static final Logger LOG = LoggerFactory.getLogger(AnchorConnection.class);
    private static final String LEADER_ANNOUNCEMENT_TOPIC = "DG/leaders";
    private MqttConnection mqttConnection;
    private InetAddress localAddress;
    private boolean active;
    private ExpiringCache<InetAddress> leaderCache = new ExpiringCache<>(1000);
    private Consumer<Message> mqttCallback;

    public AnchorConnection(InetSocketAddress anchorAddress, InetAddress localAddress) {
        this.localAddress = localAddress;
        var clientId = localAddress.getHostName();
        try {
            String serverURI = "tcp://" + anchorAddress.getHostName() + ":" + anchorAddress.getPort();
            mqttConnection = new MqttConnection(serverURI, clientId);
            mqttConnection.setMessageHandler(this::handleMqttReceive);
        } catch (MqttException e) {
            LOG.error("Could not connect to anchor: {}", e);
        }
    }

    public void startLeaderAnnouncement() {
        // Start leader announcement AND start listening to other announcements
        mqttConnection.addSubscription(LEADER_ANNOUNCEMENT_TOPIC);
        active = true;

        var announcementExecutor = Executors.newSingleThreadExecutor();
        announcementExecutor.execute(() -> {
            while (active) {
                mqttConnection.publish(new Message(LEADER_ANNOUNCEMENT_TOPIC, localAddress.getHostName().getBytes(StandardCharsets.UTF_8)));
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    public void stopLeaderAnnouncement() {
        // Stop leader announcement AND stop listening to other announcements
        mqttConnection.removeSubscription(LEADER_ANNOUNCEMENT_TOPIC);
        active = false;

        // Clear leaders
        leaderCache.clear();
    }

    public void shutdown() {
        stopLeaderAnnouncement();
        mqttConnection.disconnect();
    }

    public Set<InetAddress> getCollectedLeaders() {
        return leaderCache.getAll();
    }

    public void setMqttCallback(Consumer<Message> consumer) {
        this.mqttCallback = consumer;
    }

    public void publish(PeerMessagePublish publishMsg) {
        publishMsg.getPublishMessages().forEach(this::publish);
    }

    public void publish(MqttPublishMessage msg) {
        var topic = msg.variableHeader().topicName();
        var payload = new byte[msg.payload().readableBytes()];
        msg.payload().readBytes(payload);
        mqttConnection.publish(new Message(topic, payload));
    }

    public void addSubscription(String topicFilter) {
        mqttConnection.addSubscription(topicFilter);
    }

    private void handleLeaderReceive(Message msg) {
        var leaderHostName = new String(msg.payload, StandardCharsets.UTF_8);
        try {
            var leaderAddress = InetAddress.getByName(leaderHostName);
            if (!leaderAddress.equals(localAddress)) {
                leaderCache.add(leaderAddress);
            }
        } catch (UnknownHostException e) {
            LOG.error("Got exception while parsing leader hostname: {}", e);
        }
    }

    private void handleMqttReceive(Message msg) {
        if (Utils.mqttTopicMatchesSubscription(msg.topic, LEADER_ANNOUNCEMENT_TOPIC)) {
            handleLeaderReceive(msg);
        } else {
            mqttCallback.accept(msg);
        }
    }
}
