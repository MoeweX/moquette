package io.moquette.delaygrouping.monitoring;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ConnectionMonitor {

    private static final Logger LOG = LoggerFactory.getLogger(ConnectionMonitor.class);
    private final NioEventLoopGroup eventLoopGroup = new NioEventLoopGroup();
    private final Bootstrap bootstrap = new Bootstrap();
    private ConcurrentHashMap<InetSocketAddress, DescriptiveStatistics> monitoredPeers = new ConcurrentHashMap<>();
    private Channel channel;
    private Set<String> pendingRequests = ConcurrentHashMap.newKeySet();

    public ConnectionMonitor(int listenPort, ScheduledExecutorService executorService) {

        bootstrap
            .group(eventLoopGroup)
            .channel(NioDatagramChannel.class)
            .handler(new ChannelInitializer<DatagramChannel>() {

                @Override
                protected void initChannel(DatagramChannel channel) throws Exception {
                    ChannelPipeline pipeline = channel.pipeline();
                    pipeline.addLast("logging", new LoggingHandler(LogLevel.INFO));

                    pipeline.addLast("monitoringCodec", new MonitoringMessageCodec());
                    pipeline.addLast("monitoringHandler", new MonitoringHandler(ConnectionMonitor.this::handleMessage));
                }
            });

        bootstrap.bind(listenPort).addListener((ChannelFuture channelFuture) -> {
            if (channelFuture.isSuccess()) {
                LOG.info("Listening for PING requests on UDP port {}", listenPort);
                channel = channelFuture.channel();
            } else {
                LOG.error("Could not bind UDP port {}: {}", listenPort, channelFuture.cause());
            }
        });

        LOG.info("Starting connection monitor...");

        executorService.scheduleAtFixedRate(() -> {
            monitoredPeers.forEach((address, stats) -> {
                ping(address);
            });
        }, 0, 500, TimeUnit.MILLISECONDS);

    }

    private void handleMessage(MonitoringMessage msg) {
        String[] fields = msg.message.split(";");
        String type = fields[0];
        Long sentEpochNano = Long.valueOf(fields[1]);
        String id = fields[2];

        switch (type) {

            case "PING":
                msg.message = msg.message.replace("PING", "PONG");
                sendMessage(msg);
                break;

            case "PONG":
                boolean knownId = pendingRequests.remove(id);
                if (knownId) {
                    // calculate and save delay
                    long delay = getEpochNano() - sentEpochNano;
                    monitoredPeers.get(msg.sender).addValue(delay);
                } else {
                    LOG.warn("Received PONG message from {} with unknown id: {}", msg.sender, id);
                }
                break;

            default:
                LOG.warn("Received unexpected message: {}", msg);
        }

    }

    private void ping(InetSocketAddress recipient) {
        long epochNano = getEpochNano();

        // Save unique id
        String id = UUID.randomUUID().toString();
        pendingRequests.add(id);

        MonitoringMessage msg = new MonitoringMessage("PING;" + epochNano + ";" + id, recipient);

        sendMessage(msg);
    }

    private long getEpochNano() {
        Instant now = Instant.now();
        return (now.getEpochSecond() * 1_000_000_000L) + now.getNano();
    }

    private void sendMessage(MonitoringMessage msg) {
        channel.writeAndFlush(msg).addListener(channelFuture -> {
            if (!channelFuture.isSuccess()) {
                LOG.error("Writing into channel to {} failed: {}", msg.sender, channelFuture.cause());
            }
        });
    }

    public void shutdown() {
        eventLoopGroup.shutdownGracefully();
    }

    public void addMonitoredPeer(InetSocketAddress address) {
        monitoredPeers.put(address, new DescriptiveStatistics(20));
    }

    public void removeMonitoredPeer(InetSocketAddress address) {
        monitoredPeers.remove(address);
    }

    public Double getAverageDelay(InetSocketAddress address) {
        DescriptiveStatistics stats = monitoredPeers.get(address);
        if (stats != null) {
            return (stats.getMean() / 2) / 1000;
        } else {
            return null;
        }
    }

    public DescriptiveStatistics getStats(InetSocketAddress address) {
        return monitoredPeers.get(address);
    }
}
