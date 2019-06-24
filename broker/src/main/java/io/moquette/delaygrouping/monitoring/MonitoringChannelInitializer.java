package io.moquette.delaygrouping.monitoring;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.DatagramChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

import java.util.function.Consumer;

public class MonitoringChannelInitializer extends ChannelInitializer<DatagramChannel> {

    private Consumer<ChannelPipeline> pipelineInitializer;

    MonitoringChannelInitializer(Consumer<ChannelPipeline> pipelineInitializer) {
        this.pipelineInitializer = pipelineInitializer;
    }

    @Override
    protected void initChannel(DatagramChannel channel) throws Exception {
        ChannelPipeline pipeline = channel.pipeline();
        pipeline.addLast("logging", new LoggingHandler(LogLevel.DEBUG));

        pipeline.addLast("monitoringCodec", new MonitoringMessageCodec());

        pipelineInitializer.accept(pipeline);
    }
}
