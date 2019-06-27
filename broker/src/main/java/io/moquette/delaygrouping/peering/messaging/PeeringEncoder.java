package io.moquette.delaygrouping.peering.messaging;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.handler.codec.mqtt.MqttCodecWrapper;
import io.netty.handler.codec.mqtt.MqttPublishMessage;

import java.util.ArrayList;
import java.util.List;

/**
 * The purpose of this encoder is to convert any non-serializable PeerMessage content into a serializable payload.
 */
public class PeeringEncoder extends MessageToMessageEncoder<PeerMessage> {

    private static PeerMessagePublish encodePublish(ByteBufAllocator allocator, PeerMessagePublish msg) {
        // TODO Performance: if payload is already populated we could skip this.
        List<byte[]> serializedMessages = new ArrayList<>();
        for (MqttPublishMessage mqttPubMsg : msg.publishMessages) {
            ByteBuf byteBufMsg = MqttCodecWrapper.encodeMqttMessage(allocator, mqttPubMsg);
            mqttPubMsg.release();
            serializedMessages.add(getByteArray(byteBufMsg));
        }
        msg.payload = new ArrayList<>(serializedMessages);

        return msg;
    }

    private static byte[] getByteArray(ByteBuf byteBufMsg) {
        byte[] bytes;
        int length = byteBufMsg.readableBytes();

        // TODO Changing the default ByteBufAllocator to use array backed buffers only may have a performance impact...
        if (byteBufMsg.hasArray()) {
            bytes = byteBufMsg.array();
        } else {
            bytes = new byte[length];
            byteBufMsg.getBytes(byteBufMsg.readerIndex(), bytes);
            byteBufMsg.release();
        }

        return bytes;
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, PeerMessage msg, List<Object> out) {
        switch (msg.type) {
            case PUBLISH:
                // encode/serialize contained mqtt messages
                out.add(encodePublish(ctx.alloc(), (PeerMessagePublish) msg));
                break;
            default:
                // just forward message as there is no content to serialize
                out.add(msg);
                break;
        }
    }
}
