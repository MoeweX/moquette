package io.moquette.delaygrouping.monitoring;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ChannelHandler.Sharable
public class MonitoringEndpointHandler extends SimpleChannelInboundHandler<MonitoringMessage> {

    private static final Logger LOG = LoggerFactory.getLogger(MonitoringEndpointHandler.class);

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MonitoringMessage msg) throws Exception {
        if (!msg.message.startsWith("PING")) {
            msg.message = "Unknown message: " + msg.message;
        }

        send(msg, ctx);
    }

    private void send(MonitoringMessage message, ChannelHandlerContext ctx) {
        ctx.writeAndFlush(message)
            .addListener(channelFuture -> {
                if (!channelFuture.isSuccess()) {
                    LOG.error("Got error: {}", channelFuture.cause());
                }
            });
    }
}
