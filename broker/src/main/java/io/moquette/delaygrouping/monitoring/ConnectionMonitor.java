package io.moquette.delaygrouping.monitoring;

import io.moquette.delaygrouping.connections.ConnectionStore;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ConnectionMonitor {

    private static final Logger LOG = LoggerFactory.getLogger(ConnectionMonitor.class);
    private final NioEventLoopGroup clientGroup = new NioEventLoopGroup();
    private final NioEventLoopGroup serverGroup = new NioEventLoopGroup();
    private ConcurrentHashMap<InetSocketAddress, DescriptiveStatistics> monitoredPeers = new ConcurrentHashMap<>();
    private ConnectionStore<MonitoringConnection> connectionStore = new ConnectionStore<>();
    private final Bootstrap clientBootstrap = new Bootstrap();
    private final Bootstrap serverBootstrap = new Bootstrap();

    public ConnectionMonitor(int listenPort, ScheduledExecutorService executorService) {

        // Start Echo listener
        serverBootstrap
            .group(serverGroup)
            .channel(NioDatagramChannel.class)
            .handler(new MonitoringChannelInitializer(pipeline -> {
                pipeline.addLast(new MonitoringEndpointHandler());
            }));

        serverBootstrap.bind(listenPort).addListener((ChannelFuture channelFuture) -> {
            if (channelFuture.isSuccess()) {
                LOG.info("Listening for PING requests on UDP port {}", listenPort);
            } else {
                LOG.error("Could not bind UDP port {}: {}", listenPort, channelFuture.cause());
            }
        });

        // Start connection probing
        clientBootstrap
            .group(clientGroup)
            .channel(NioDatagramChannel.class)
            .handler(new MonitoringChannelInitializer(pipeline -> {
                MonitoringConnection connection = new MonitoringConnection(pipeline.channel());
                pipeline.addLast("monitoringHandler", new MonitoringHandler(connection));
            }));

        LOG.info("Starting connection monitor...");

        executorService.scheduleAtFixedRate(() -> {

            connectionStore.missingConnections().forEach(address -> {
                clientBootstrap.connect(address).addListener((ChannelFuture connectFuture) -> {
                    if (connectFuture.isSuccess()) {
                        Channel channel = connectFuture.channel();

                        MonitoringHandler monitoringHandler = channel.pipeline().get(MonitoringHandler.class);
                        monitoringHandler.getConnection().setIntendedRemoteAddress(address);

                        LOG.info("Established bridge channel to {}", address);

                        channel.closeFuture().addListener((ChannelFuture closeFuture) -> {
                            LOG.info("Channel to {} has closed.", address);
                        });
                    }
                });
            });

        }, 1, 5, TimeUnit.SECONDS);
    }

    public void shutdown() {
        clientGroup.shutdownGracefully();
    }

    public void addMonitoredPeer(InetSocketAddress address) {
        connectionStore.addIntendedConnection(address);
    }

    public void removeMonitoredPeer(InetSocketAddress address) {
        connectionStore.removeIntendedConnection(address);
    }
}
