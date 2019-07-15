package io.moquette.bridge;

import io.moquette.bridge.messaging.BridgeMessage;
import io.moquette.bridge.messaging.BridgeMessageConnAck;
import io.moquette.bridge.messaging.BridgeMessageConnect;
import io.moquette.bridge.messaging.BridgeMessagePublish;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Objects;

public class BridgeConnection {

    private static final Logger LOG = LoggerFactory.getLogger(BridgeConnection.class);
    private final Channel channel;
    private final String bridgeId;
    private String remoteBridgeId = null;
    private InetSocketAddress intendedRemoteAddress;
    private Bridge parent;
    private ConnectionStatus status = ConnectionStatus.NOT_CONNECTED;

    BridgeConnection(Channel channel, String bridgeId, Bridge parent) {
        this.bridgeId = bridgeId;
        this.parent = parent;
        this.channel = channel;
    }

    void handleChannelActive() {
        if (isClient()) {
            connect();
            status = ConnectionStatus.PENDING;
        }
    }

    void handleChannelInactive() {
        status = ConnectionStatus.NOT_CONNECTED;
    }

    void handleChannelException() {
        status = ConnectionStatus.NOT_CONNECTED;
    }

    void handleMessage(BridgeMessage message) {
        LOG.info("Received message type {}: {}", message.type.name(), message);
        switch (message.type) {
            case CONNECT:
                handleConnect((BridgeMessageConnect) message);
                break;
            case CONNACK:
                handleConnAck((BridgeMessageConnAck) message);
                break;
            case PUBLISH:
                handlePublish((BridgeMessagePublish) message);
                break;
            default:
                LOG.error("Received invalid message type {}", message.type.name());
        }
    }

    private void connect() {
        LOG.info("Sending CONNECT to bridge peer {}", channel.remoteAddress().toString());
        writeAndFlush(new BridgeMessageConnect(bridgeId));
    }


    private void handleConnect(BridgeMessageConnect connMsg) {
        status = ConnectionStatus.CONNECTED;
        remoteBridgeId = connMsg.bridgeId;
        parent.notifyConnectionEstablished(this);
        LOG.info("Sending CONNACK to bridge peer {}", channel.remoteAddress().toString());
        writeAndFlush(new BridgeMessageConnAck(bridgeId));
    }

    private void handleConnAck(BridgeMessageConnAck connAckMsg) {
        status = ConnectionStatus.CONNECTED;
        remoteBridgeId = connAckMsg.bridgeId;
        parent.notifyConnectionEstablished(this);
    }

    private void handlePublish(BridgeMessagePublish incomingMessage) {
        LOG.debug("Received message to handlePublish: {}", incomingMessage);
        LOG.info("Doing internal publish to clients...");
        for (MqttPublishMessage mqttPubMsg : incomingMessage.getPublishMessages()) {
            // Internal handlePublish to interested clients
            parent.getMessageLogger().log(mqttPubMsg);
            parent.internalPublish(mqttPubMsg);
        }
    }

    void writeAndFlush(BridgeMessage msg) {
        msg.retain();
        channel.writeAndFlush(msg).addListener((ChannelFuture writeFuture) -> {
            if (!writeFuture.isSuccess() && !writeFuture.isCancelled()) {
                LOG.error("Failed writing into channel to {}", channel.remoteAddress().toString(), writeFuture.cause());
            }
        });
    }

    public Boolean isConnected() {
        return status == ConnectionStatus.CONNECTED;
    }

    public Boolean isClient() {
        return intendedRemoteAddress != null;
    }

    public String getRemoteId() {
        return remoteBridgeId;
    }

    public InetSocketAddress getRemoteAddress() {
        return (InetSocketAddress) channel.remoteAddress();
    }

    public void setRemoteAddress(InetSocketAddress address) {
        this.intendedRemoteAddress = address;
    }

    @Override
    public String toString() {
        return "BridgeConnection(" + status.name() + "): " + channel.localAddress() + "(" + bridgeId + ") <-> " + channel.remoteAddress() + "(" + remoteBridgeId + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BridgeConnection that = (BridgeConnection) o;
        return Objects.equals(remoteBridgeId, that.remoteBridgeId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(remoteBridgeId);
    }
}
