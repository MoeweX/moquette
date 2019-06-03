package io.moquette.bridge;

import io.moquette.bridge.messaging.BridgeMessage;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BridgeHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(BridgeHandler.class);

    private BridgeConnection connection;

    BridgeHandler(BridgeConnection connection) {
        this.connection = connection;
    }

    public BridgeConnection getConnection() {
        return connection;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        BridgeMessage message = (BridgeMessage) msg;
        LOG.debug("Channel to {} has read message: {}", ctx.channel().remoteAddress(), message.toString());
        connection.handleMessage(message);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        LOG.debug("Channel with remote address {} has become active.", ctx.channel().remoteAddress());
        connection.handleChannelActive();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        LOG.debug("Channel to {} has become inactive.", ctx.channel().remoteAddress());
        connection.handleChannelInactive();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOG.error("Unexpected channel exception.", cause);
        ctx.close().addListener(future -> LOG.error("Channel was closed due to exception."));
        connection.handleChannelException();
    }

}
