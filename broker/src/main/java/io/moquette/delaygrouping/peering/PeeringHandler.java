package io.moquette.delaygrouping.peering;

import io.moquette.bridge.BridgeConnection;
import io.moquette.bridge.messaging.BridgeMessage;
import io.moquette.delaygrouping.peering.messaging.PeerMessage;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PeeringHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(PeeringHandler.class);

    private PeerConnection connection;

    void setConnection(PeerConnection connection) {
        this.connection = connection;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        PeerMessage message = (PeerMessage) msg;
        LOG.debug("Channel to {} has read message: {}", ctx.channel().remoteAddress(), message.toString());
        connection.handleMessage(message);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        LOG.debug("Channel with remote address {} has become active.", ctx.channel().remoteAddress());
        //connection.handleChannelActive();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        LOG.debug("Channel to {} has become inactive.", ctx.channel().remoteAddress());
        connection.handleChannelInactive(ctx.channel());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOG.error("Unexpected channel exception.", cause);
        ctx.close().addListener(future -> LOG.error("Channel was closed due to exception."));
        connection.handleChannelException(ctx.channel());
    }

}
