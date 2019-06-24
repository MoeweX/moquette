package io.moquette.delaygrouping.monitoring;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.CharsetUtil;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ConnectionMonitor {

    private static final Logger LOG = LoggerFactory.getLogger(ConnectionMonitor.class);
    private ConcurrentHashMap<InetSocketAddress, DescriptiveStatistics> monitoredPeers = new ConcurrentHashMap<>();
    private ChannelInitializer<SocketChannel> channelInitializer = new ChannelInitializer<SocketChannel>() {

        @Override
        protected void initChannel(SocketChannel channel) {
            ChannelPipeline pipeline = channel.pipeline();
            pipeline.addLast("logging", new LoggingHandler(LogLevel.DEBUG));

            pipeline.addLast("frameDecoder", new LineBasedFrameDecoder(80));
            pipeline.addLast("stringDecoder", new StringDecoder(CharsetUtil.UTF_8));
            pipeline.addLast("stringEncoder", new StringEncoder(CharsetUtil.UTF_8));

            pipeline.addLast("monitoringHandler", null);
        }
    };

    public ConnectionMonitor(int listenPort, ScheduledExecutorService executorService) {
        ServerBootstrap serverBootstrap = new ServerBootstrap();
        Bootstrap clientBootstrap = new Bootstrap();

        NioEventLoopGroup clientGroup = new NioEventLoopGroup();
        clientBootstrap
            .group(clientGroup)
            .channel(NioSocketChannel.class)
            .option(ChannelOption.SO_KEEPALIVE, true)
            .option(ChannelOption.TCP_NODELAY, true)
            .handler(channelInitializer);

        NioEventLoopGroup serverParentGroup = new NioEventLoopGroup();
        NioEventLoopGroup serverChildGroup = new NioEventLoopGroup();
        serverBootstrap
            .group(serverParentGroup, serverChildGroup)
            .channel(NioServerSocketChannel.class)
            .option(ChannelOption.SO_KEEPALIVE, true)
            .option(ChannelOption.SO_REUSEADDR, true)
            .childOption(ChannelOption.SO_KEEPALIVE, true)
            .childOption(ChannelOption.TCP_NODELAY, true)
            .childHandler(channelInitializer);

        LOG.info("Starting connection monitor...");

        serverBootstrap.bind(listenPort).addListener(channelFuture -> {
            if (channelFuture.isSuccess()) {
                LOG.info("Connection monitor listening on port {}", listenPort);
            }
        });

        executorService.scheduleAtFixedRate(() -> {

        }, 1, 5, TimeUnit.SECONDS);
    }

    public void addMonitoredPeer(InetSocketAddress address) {
        monitoredPeers.put(address, new DescriptiveStatistics(20));
    }
}
