package io.moquette.delaygrouping.peering;

import io.moquette.delaygrouping.peering.messaging.PeeringDecoder;
import io.moquette.delaygrouping.peering.messaging.PeeringEncoder;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

public class PeerConnectionManager {
    private static final Logger LOG = LoggerFactory.getLogger(PeerConnectionManager.class);
    private Map<InetAddress, PeerConnection> connections = new ConcurrentHashMap<>();
    private EventLoopGroup clientGroup;
    private EventLoopGroup serverParentGroup;
    private EventLoopGroup serverChildGroup;
    private Bootstrap bootstrap;
    private ServerBootstrap serverBootstrap;
    private int bindAndConnectPort = 1884;
    private Consumer<PeerConnection> newConnectionHandler;
    private ExecutorService notificationExecutor = Executors.newCachedThreadPool();

    public PeerConnectionManager(InetAddress bindAddress, Consumer<PeerConnection> newConnectionHandler) {
        this.newConnectionHandler = newConnectionHandler;
        initializeClientBootstrap(bindAddress);
        initializeServerBootstrap();

        startListener(bindAddress);
    }

    private static ChannelInitializer<SocketChannel> createChannelInitializer(Consumer<SocketChannel> peeringHandlerAdder) {
        return new ChannelInitializer<>() {

            @Override
            protected void initChannel(SocketChannel channel) {
                var pipeline = channel.pipeline();
                //pipeline.addLast("logging", new LoggingHandler(LogLevel.INFO));
                pipeline.addLast("objectDecoder", new ObjectDecoder(ClassResolvers.cacheDisabled(null)));
                pipeline.addLast("objectEncoder", new ObjectEncoder());
                pipeline.addLast("bridgeDecoder", new PeeringDecoder());
                pipeline.addLast("bridgeEncoder", new PeeringEncoder());

                peeringHandlerAdder.accept(channel);
            }
        };
    }

    public PeerConnection getConnectionToPeer(InetAddress peerAddress) {
        return getOrCreateConnection(peerAddress);
    }

    public void closeConnectionToPeer(InetAddress peerAddress) {
        var connection = connections.remove(peerAddress);
        if (connection != null) {
            connection.close();
        }
    }

    public void shutdown() {
        clientGroup.shutdownGracefully();
        serverParentGroup.shutdownGracefully();
        serverChildGroup.shutdownGracefully();
    }

    private void startListener(InetAddress bindAddress) {
        serverBootstrap.bind(bindAddress, bindAndConnectPort).addListener(channelFuture -> {
            if (channelFuture.isSuccess()) {
                LOG.info("Peer listener bound to port {}", bindAndConnectPort);
            } else {
                LOG.error("Could not bind peer listener to port {}: {}", bindAndConnectPort, channelFuture.cause());
            }
        });
    }

    private void initializeClientBootstrap(InetAddress bindAddress) {
        clientGroup = new NioEventLoopGroup();
        bootstrap = new Bootstrap();
        bootstrap
            .group(clientGroup)
            .channel(NioSocketChannel.class)
            .option(ChannelOption.SO_KEEPALIVE, true)
            .localAddress(new InetSocketAddress(bindAddress, 0))
            .handler(createChannelInitializer(channel -> {
                // We'll add the connection from within the success listener (see PeerConnection)
                channel.pipeline().addLast("peeringHandler", new PeeringHandler());
            }));
    }

    private void initializeServerBootstrap() {
        serverParentGroup = new NioEventLoopGroup();
        serverChildGroup = new NioEventLoopGroup();
        serverBootstrap = new ServerBootstrap();
        serverBootstrap
            .group(serverParentGroup, serverChildGroup)
            .channel(NioServerSocketChannel.class)
            .option(ChannelOption.SO_REUSEADDR, true)
            .option(ChannelOption.SO_BACKLOG, 128)
            .childOption(ChannelOption.SO_KEEPALIVE, true)
            .childOption(ChannelOption.TCP_NODELAY, true)
            .childHandler(createChannelInitializer(channel -> {
                var peerConnection = getOrCreateConnection(channel.remoteAddress().getAddress());
                if (peerConnection.getChannelCount() == 0) newConnectionHandler.accept(peerConnection);
                peerConnection.addChannel(channel);
                var peeringHandler = new PeeringHandler();
                peeringHandler.setConnection(peerConnection);
                channel.pipeline().addLast("peeringHandler", peeringHandler);
            }));
    }

    private PeerConnection getOrCreateConnection(InetAddress peerAddress) {
        return connections.compute(peerAddress, (key, existingConnection) -> {
            if (existingConnection == null) {
                return new PeerConnection(bootstrap, new InetSocketAddress(peerAddress, bindAndConnectPort), newConnectionHandler);
            } else {
                return existingConnection;
            }
        });
    }
}
