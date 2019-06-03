package io.netty.handler.codec.mqtt;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class MqttCodecWrapper {

    private static final Logger LOG = LoggerFactory.getLogger(MqttCodecWrapper.class);

    public static ByteBuf encodeMqttMessage(ByteBufAllocator allocator, MqttMessage message) {
        return MqttEncoder.doEncode(allocator, message);
    }

    public static MqttPublishMessage decodeMqttPublishMessage(ByteBuf serializedMsg) {
        List<Object> outList = new ArrayList<>();
        try {
            // TODO This may induce a serious performance hit (we could try subclassing MqttDecoder and working around its statefulness)
            (new MqttDecoder()).decode(null, serializedMsg, outList);
            LOG.info("Decoded MQTT message: {}", outList.get(0));
            return (MqttPublishMessage) outList.get(0);
        } catch (Exception ex) {
            LOG.error("Error while decoding MqttPublishMessage.", ex);
        }
        return null;
    }
}
