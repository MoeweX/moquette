package io.moquette.delaygrouping.monitoring;

import io.moquette.delaygrouping.connections.Connection;
import io.moquette.delaygrouping.connections.ConnectionStatus;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.time.Instant;

public class MonitoringConnection implements Connection {

    private static final Logger LOG = LoggerFactory.getLogger(MonitoringConnection.class);

    private String remoteId;
    private Channel channel;
    private InetSocketAddress intendedRemoteAddress;
    private ConnectionStatus status;

    public MonitoringConnection(Channel channel) {
        this.channel = channel;
        this.status = ConnectionStatus.NOT_CONNECTED;
    }

    void handleChannelRead(String message) {

    }

    private void sendPing() {
        // TODO Add unique id and keep track of it...
        Instant now = Instant.now();
        long epochNano = (now.getEpochSecond() * 1_000_000_000) + now.getNano();

        writeAndFlush(epochNano + ";PING");
    }

    private void writeAndFlush(String message) {
        channel.writeAndFlush(message).addListener(channelFuture -> {
            if (!channelFuture.isSuccess()) {
                LOG.error("Writing into channel to {} failed: {}", channel.remoteAddress(), channelFuture.cause());
            }
        });
    }

    @Override
    public String getRemoteId() {
        return remoteId;
    }

    @Override
    public InetSocketAddress getRemoteAddress() {
        return (InetSocketAddress) channel.remoteAddress();
    }

    @Override
    public InetSocketAddress getIntendedRemoteAddress() {
        return intendedRemoteAddress;
    }

    @Override
    public void setIntendedRemoteAddress(InetSocketAddress address) {
        this.intendedRemoteAddress = address;
    }

    @Override
    public Boolean isConnected() {
        return status.equals(ConnectionStatus.CONNECTED);
    }

    @Override
    public Boolean isClient() {
        return null;
    }
}
