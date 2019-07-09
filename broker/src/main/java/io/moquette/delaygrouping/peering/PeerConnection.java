package io.moquette.delaygrouping.peering;

import io.moquette.delaygrouping.peering.messaging.PeerMessage;
import io.moquette.delaygrouping.peering.messaging.PeerMessageType;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOutboundInvoker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class PeerConnection {
    private static final Logger LOG = LoggerFactory.getLogger(PeerConnection.class);

    private Set<BiConsumer<PeerMessage, PeerConnection>> handlers = ConcurrentHashMap.newKeySet();
    private Map<PeerMessageType, Set<BiConsumer<PeerMessage, PeerConnection>>> handlersByType = new ConcurrentHashMap<>();
    private List<Channel> channels = new ArrayList<>();
    private Bootstrap bootstrap;
    private InetSocketAddress remoteAddress;
    private LinkedBlockingDeque<PeerMessage> sendQueue = new LinkedBlockingDeque<>();
    private ExecutorService executor = Executors.newCachedThreadPool();
    private boolean running = false;
    private Consumer<PeerConnection> newConnectionHandler;

    private PeerConnection(Bootstrap bootstrap) {
        this.bootstrap = bootstrap;

        // Initialize handlers by type
        for (PeerMessageType type : PeerMessageType.values()) {
            handlersByType.put(type, ConcurrentHashMap.newKeySet());
        }

        // Start message sending thread
        executor.execute(() -> {
            running = true;
            while (running) {
                PeerMessage msg = null;
                try {
                    msg = sendQueue.take();
                    connect().get();
                    writeAndFlush(msg).get();
                } catch (ExecutionException e) {
                    // The sending process hasn't worked, so put the message back into the queue
                    sendQueue.addFirst(msg);
                } catch (InterruptedException ignored) {
                }
            }
        });
    }

    PeerConnection(Bootstrap bootstrap, InetSocketAddress remoteAddress, Consumer<PeerConnection> newConnectionHandler) {
        this(bootstrap);
        this.remoteAddress = remoteAddress;
        this.newConnectionHandler = newConnectionHandler;
    }

    private Future connect() {
        var future = new CompletableFuture<>();
        if (channels.isEmpty()) {
            bootstrap.connect(remoteAddress).addListener((ChannelFuture channelFuture) -> {
                if (channelFuture.isSuccess()) {
                    var peeringHandler = channelFuture.channel().pipeline().get(PeeringHandler.class);
                    peeringHandler.setConnection(this);
                    if (channels.size() == 0) newConnectionHandler.accept(this);
                    channels.add(channelFuture.channel());
                    future.complete(null);
                } else {
                    LOG.error("Could not establish connection to {}: {}", remoteAddress, channelFuture.cause());
                    future.completeExceptionally(channelFuture.cause());
                }
            });
        } else {
            future.complete(null);
        }
        return future;
    }

    private Future writeAndFlush(PeerMessage msg) {
        var future = new CompletableFuture<>();

        // Always use the first channel, as it should always be there and shouldn't make a difference anyway
        channels.get(0).writeAndFlush(msg).addListener((ChannelFuture channelFuture) -> {
            if (channelFuture.isSuccess()) {
                future.complete(null);
            } else {
                LOG.error("Failed writing into channel to {}", channelFuture.channel().remoteAddress().toString(), channelFuture.cause());
                future.completeExceptionally(channelFuture.cause());
            }
        });

        return future;
    }

    void addChannel(Channel channel) {
        channels.add(channel);
    }

    int getChannelCount() {
        return channels.size();
    }

    void handleMessage(PeerMessage msg) {
        //LOG.info("Received peer message from {}: {}", remoteAddress.getHostString(), msg);
        handlersByType.get(msg.type)
            .forEach(consumer -> consumer.accept(msg, this));
    }

    public void sendMessage(PeerMessage msg) {
        LOG.info("Sending peer message to {}: {}", remoteAddress.getHostString(), msg);
        msg.retain();
        sendQueue.add(msg);
    }

    public void registerMessageHandler(BiConsumer<PeerMessage, PeerConnection> handler, PeerMessageType... messageTypes) {
        // listen for a specific message type (in order to isolate or react to a certain type of communication)
        // there can be multiple overlapping handlers
        // What about threading? Call each on a separate thread?
        handlers.add(handler);
        for (PeerMessageType type : messageTypes) {
            handlersByType.get(type).add(handler);
        }
        LOG.debug("Successfully registered message handlers for connection to {}", remoteAddress.getHostString());
    }

    public InetAddress getRemoteAddress() {
        return remoteAddress.getAddress();
    }

    void close() {
        running = false;
        channels.forEach(ChannelOutboundInvoker::close);
    }

    void handleChannelInactive(Channel channel) {
        channels.remove(channel);
        if (channel != null) {
            channel.close();
        }
    }

    void handleChannelException(Channel channel) {
        channels.remove(channel);
        if (channel != null) {
            channel.close();
        }
    }
}
