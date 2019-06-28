package io.moquette.delaygrouping.peering;

import io.moquette.delaygrouping.peering.messaging.PeerMessage;
import io.moquette.delaygrouping.peering.messaging.PeerMessageType;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOutboundInvoker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public class PeerConnection {
    private static final Logger LOG = LoggerFactory.getLogger(PeerConnection.class);

    private Map<String, Consumer<PeerMessage>> handlers = new ConcurrentHashMap<>();
    private Map<PeerMessageType, List<Consumer<PeerMessage>>> handlersByType = new ConcurrentHashMap<>();
    private List<Channel> channels = new ArrayList<>();
    private Bootstrap bootstrap;
    private InetSocketAddress remoteAddress;

    // TODO Auto reconnect?

    PeerConnection(Bootstrap bootstrap) {
        this.bootstrap = bootstrap;

        // Initialize handlers by type
        for (PeerMessageType type : PeerMessageType.values()) {
            handlersByType.put(type, new ArrayList<>());
        }
    }

    PeerConnection(Bootstrap bootstrap, InetSocketAddress remoteAddress) {
        this(bootstrap);
        this.remoteAddress = remoteAddress;
    }

    void connect() {
        if (channels.isEmpty()) {
            bootstrap.connect(remoteAddress).addListener((ChannelFuture channelFuture) -> {
                channels.add(channelFuture.channel());
                var peeringHandler = channelFuture.channel().pipeline().get(PeeringHandler.class);
                peeringHandler.setConnection(this);
            });
        }
    }

    void addChannel(Channel channel) {
        channels.add(channel);
    }

    void handleMessage(PeerMessage msg) {
        handlersByType.get(msg.type)
            .forEach(consumer -> consumer.accept(msg));
    }

    public void sendMessage(PeerMessage msg) {
        // TODO Should we autoconnect here? Or maybe just add the message to an internal queue
        msg.retain();
        // Always use the first channel, as it should always be there and shouldn't make a difference anyway
        channels.get(0).writeAndFlush(msg).addListener((ChannelFuture channelFuture) -> {
            if (!channelFuture.isSuccess() && !channelFuture.isCancelled()) {
                LOG.error("Failed writing into channel to {}", channelFuture.channel().remoteAddress().toString(), channelFuture.cause());
            }
        });
    }

    public String registerMessageHandler(Consumer<PeerMessage> handler, PeerMessageType... messageTypes) {
        // listen for a specific message type (in order to isolate or react to a certain type of communication)
        // there can be multiple overlapping handlers
        // What about threading? Call each on a separate thread?
        String handlerId = UUID.randomUUID().toString();
        handlers.put(handlerId, handler);
        for (PeerMessageType type : messageTypes) {
            handlersByType.get(type).add(handler);
        }
        return handlerId;
    }

    public void removeMessageHandler(String handlerId) {
        Consumer<PeerMessage> handler = handlers.remove(handlerId);
        if (handler != null) {
            for (PeerMessageType type : PeerMessageType.values()) {
                handlersByType.get(type).remove(handler);
            }
        }
    }

    void close() {
        channels.forEach(ChannelOutboundInvoker::close);
    }

    void handleChannelInactive(Channel channel) {
        channels.remove(channel);
        channel.close();
    }

    void handleChannelException(Channel channel) {
        channels.remove(channel);
        channel.close();
    }
}
