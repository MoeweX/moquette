package io.moquette.bridge.messaging;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.mqtt.MqttCodecWrapper;
import io.netty.handler.codec.mqtt.MqttPublishMessage;

import java.util.ArrayList;
import java.util.List;

/**
 * The purpose of this decoder is to decode any BridgeMessage payload into its original form (i.e. potentially non-serializable fields).
 */
public class BridgeDecoder extends MessageToMessageDecoder<BridgeMessage> {

    private static BridgeMessagePublish decodePublish(BridgeMessagePublish msg) {
        List<MqttPublishMessage> deserializedMessages = new ArrayList<>();
        for (byte[] serializedMqttPub : msg.payload) {
            ByteBuf byteBufMsg = Unpooled.wrappedBuffer(serializedMqttPub);
            deserializedMessages.add(MqttCodecWrapper.decodeMqttPublishMessage(byteBufMsg));
        }

        msg.publishMessages = deserializedMessages;

        return msg;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, BridgeMessage msg, List<Object> out) throws Exception {
        switch (msg.type) {
            case PUBLISH:
                // decode/deserialize contained mqtt messages
                out.add(decodePublish((BridgeMessagePublish) msg));
                break;
            default:
                // just forward as there is no content to decode
                out.add(msg);
                break;
        }
    }

}
