package io.moquette.delaygrouping.monitoring;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DatagramPacket;
import io.netty.handler.codec.MessageToMessageCodec;

import java.nio.charset.StandardCharsets;
import java.util.List;

@ChannelHandler.Sharable
public class MonitoringMessageCodec extends MessageToMessageCodec<DatagramPacket, MonitoringMessage> {

    @Override
    protected void encode(ChannelHandlerContext ctx, MonitoringMessage msg, List<Object> out) {
        out.add(new DatagramPacket(Unpooled.copiedBuffer(msg.message, StandardCharsets.UTF_8), msg.sender));
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, DatagramPacket msg, List<Object> out) {
        out.add(new MonitoringMessage(msg.content().toString(StandardCharsets.UTF_8), msg.sender()));
    }
}
