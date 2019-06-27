package io.moquette.delaygrouping.peering.messaging;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.mqtt.MqttCodecWrapper;
import io.netty.handler.codec.mqtt.MqttPublishMessage;

import java.util.ArrayList;
import java.util.List;

/**
 * The purpose of this decoder is to decode any PeerMessage payload into its original form (i.e. potentially non-serializable fields).
 */
public class PeeringDecoder extends MessageToMessageDecoder<PeerMessage> {

    private static PeerMessagePublish decodePublish(PeerMessagePublish msg) {
        List<MqttPublishMessage> deserializedMessages = new ArrayList<>();
        for (byte[] serializedMqttPub : msg.payload) {
            ByteBuf byteBufMsg = Unpooled.wrappedBuffer(serializedMqttPub);
            MqttPublishMessage message = MqttCodecWrapper.decodeMqttPublishMessage(byteBufMsg);
            if (message != null) deserializedMessages.add(message);
        }

        msg.publishMessages = deserializedMessages;

        return msg;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, PeerMessage msg, List<Object> out) throws Exception {
        switch (msg.type) {
            case PUBLISH:
                // decode/deserialize contained mqtt messages
                out.add(decodePublish((PeerMessagePublish) msg));
                break;
            default:
                // just forward as there is no content to decode
                out.add(msg);
                break;
        }
    }

}
