package io.moquette.bridge;

import io.moquette.bridge.messaging.BridgeDecoder;
import io.moquette.bridge.messaging.BridgeEncoder;
import io.moquette.bridge.messaging.BridgeMessagePublish;
import io.moquette.server.Server;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Bridge {

    private static final Logger LOG = LoggerFactory.getLogger(Bridge.class);
    private final String bridgeId = UUID.randomUUID().toString();

    private final Bootstrap bootstrap = new Bootstrap();
    private final ServerBootstrap serverBootstrap = new ServerBootstrap();
    private final ConnectionStore connectionStore;
    private final Server server;
    private EventLoopGroup serverParentGroup;
    private EventLoopGroup serverChildGroup;
    private EventLoopGroup clientGroup;
    private ChannelInitializer<SocketChannel> channelInitializer = new ChannelInitializer<SocketChannel>() {

        @Override
        protected void initChannel(SocketChannel channel) {
            ChannelPipeline pipeline = channel.pipeline();
            pipeline.addLast("logging", new LoggingHandler(LogLevel.DEBUG));
            pipeline.addLast("objectDecoder", new ObjectDecoder(ClassResolvers.cacheDisabled(null)));
            pipeline.addLast("objectEncoder", new ObjectEncoder());
            pipeline.addLast("bridgeDecoder", new BridgeDecoder());
            pipeline.addLast("bridgeEncoder", new BridgeEncoder());

            BridgeConnection connection = createConnection(channel);
            pipeline.addLast("bridgeHandler", new BridgeHandler(connection));
        }
    };

    public Bridge(BridgeConfiguration config, Server server, ScheduledExecutorService scheduler) {
        this.server = server;
        this.connectionStore = new ConnectionStore(config.getBridgePeers());
        initializeClientBootstrap();
        initializeServerBootstrap();

        LOG.info("Starting bridge...");

        serverBootstrap.bind(config.getPort())
            .addListener((ChannelFuture future) -> {
                if (future.isSuccess()) {
                    LOG.info("Bridge bound to {}", future.channel().localAddress());
                }
            });

        // This shit could be refactored into something more elegant (e.g. event-based, s.t. connections can retry themselves)
        scheduler.scheduleAtFixedRate(() -> {
            List<InetSocketAddress> missingConnections = connectionStore.missingConnections();
            LOG.debug("Currently active connections: {}", connectionStore.getAllDistinct());
            LOG.debug("Detected missing connections to: {}", missingConnections);
            for (InetSocketAddress address : missingConnections) {
                LOG.info("Trying to connect to bridge peer: {}", address);
                bootstrap.connect(address).addListener((ChannelFuture connectFuture) -> {
                    if (connectFuture.isSuccess()) {
                        Channel channel = connectFuture.channel();
                        InetSocketAddress remoteAddress = (InetSocketAddress) channel.remoteAddress();
                        LOG.info("Established bridge channel to {}", remoteAddress);

                        // This is ugly! We could avoid this by separating BridgeClientHandler and BridgeServerHandler (only the client sends initial CONNECT).
                        // We would then still need to set the configured address that was used to connect because we're using that now for the ConnectionStore.
                        BridgeHandler bridgeHandler = channel.pipeline().get(BridgeHandler.class);
                        bridgeHandler.getConnection().setRemoteAddress(address);

                        channel.closeFuture().addListener((ChannelFuture closeFuture) -> {
                            LOG.info("Channel to {} has closed.", remoteAddress);
                        });
                    } else {
                        LOG.info("Connection to {} failed: {}", connectFuture.channel().remoteAddress(), connectFuture.cause());
                    }

                });
            }
        }, 1, 5, TimeUnit.SECONDS);
    }

    public void shutdown() {
        serverChildGroup.shutdownGracefully();
        serverParentGroup.shutdownGracefully();
        clientGroup.shutdownGracefully();
    }

    public String getBridgeId() {
        return bridgeId;
    }

    public List<String> getConnectedBridges() {
        return connectionStore.getAllDistinctRemoteIds();
    }

    private void initializeClientBootstrap() {
        clientGroup = new NioEventLoopGroup();
        bootstrap
            .group(clientGroup)
            .channel(NioSocketChannel.class)
            .option(ChannelOption.SO_KEEPALIVE, true)
            .handler(channelInitializer);
    }

    private void initializeServerBootstrap() {
        serverParentGroup = new NioEventLoopGroup();
        serverChildGroup = new NioEventLoopGroup();
        serverBootstrap
            .group(serverParentGroup, serverChildGroup)
            .channel(NioServerSocketChannel.class)
            .option(ChannelOption.SO_KEEPALIVE, true)
            .option(ChannelOption.SO_REUSEADDR, true)
            .option(ChannelOption.SO_BACKLOG, 128)
            .childOption(ChannelOption.SO_KEEPALIVE, true)
            .childOption(ChannelOption.TCP_NODELAY, true)
            .childHandler(channelInitializer);
    }

    private BridgeConnection createConnection(Channel channel) {
        return new BridgeConnection(channel, bridgeId, this);
    }

    void notifyConnectionEstablished(BridgeConnection newConnection) {
        LOG.info("Established {}", newConnection);
        connectionStore.addActualConnection(newConnection);
    }

    void bridgePublish(BridgeMessagePublish message) {
        for (BridgeConnection connection : connectionStore.getAllDistinct()) {
            if (connection.isConnected()) {
                connection.writeAndFlush(message);
            }
        }
    }

    void internalPublish(MqttPublishMessage message) {
        server.internalPublish(message, null);
    }

}
