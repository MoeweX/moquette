package io.moquette.delaygrouping.monitoring;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.function.Consumer;

public class MonitoringHandler extends SimpleChannelInboundHandler<MonitoringMessage> {

    private Consumer<MonitoringMessage> messageConsumer;

    public MonitoringHandler(Consumer<MonitoringMessage> messageConsumer) {
        this.messageConsumer = messageConsumer;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MonitoringMessage msg) {
        messageConsumer.accept(msg);
    }
}
