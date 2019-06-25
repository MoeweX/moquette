package io.moquette.bridge.messaging;

import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import static org.assertj.core.api.Assertions.assertThat;

import io.netty.handler.codec.mqtt.MqttPublishMessage;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BridgeCodecTest {

    @Test
    public void testConnect() throws Exception {
        // Create mock instances for encoder test
        ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
        List<Object> mockOut = new ArrayList<>();
        List<Object> resultOut = new ArrayList<>();

        // Create bridge connect message
        BridgeMessageConnect msg = new BridgeMessageConnect("testBridgeId");

        // Encode bridge connect message
        BridgeEncoder encoder = new BridgeEncoder();
        encoder.encode(mockCtx, msg, mockOut);

        // Decode bridge connect message
        BridgeDecoder decoder = new BridgeDecoder();
        decoder.decode(mockCtx, (BridgeMessage) mockOut.get(0), resultOut);

        // Compare result of decoding with original message
        assertThat(msg).isEqualToComparingFieldByFieldRecursively(resultOut.get(0));
    }

    @Test
    public void testConnAck() throws Exception {
        // Create mock instances for encoder test
        ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
        List<Object> mockOut = new ArrayList<>();
        List<Object> resultOut = new ArrayList<>();

        // Create bridge connect message
        BridgeMessageConnAck msg = new BridgeMessageConnAck("testBridgeId");

        // Encode bridge connect message
        BridgeEncoder encoder = new BridgeEncoder();
        encoder.encode(mockCtx, msg, mockOut);

        // Decode bridge connect message
        BridgeDecoder decoder = new BridgeDecoder();
        decoder.decode(mockCtx, (BridgeMessage) mockOut.get(0), resultOut);

        // Compare result of decoding with original message
        assertThat(msg).isEqualToComparingFieldByFieldRecursively(resultOut.get(0));
    }

    @Test
    public void testPublish() throws Exception {
        // Create mock instances for encoder test
        ChannelHandlerContext mockCtx = mock(ChannelHandlerContext.class);
        when(mockCtx.alloc()).thenReturn(new UnpooledByteBufAllocator(false));
        List<Object> mockOut = new ArrayList<>();
        List<Object> resultOut = new ArrayList<>();

        // Create MQTT messages and bridge publish message
        MqttPublishMessage mqttPub1 = BridgeMessageFactory.publish("testtest1", Unpooled.wrappedBuffer("testtest1".getBytes()));
        MqttPublishMessage mqttPub2 = BridgeMessageFactory.publish("testtest2", Unpooled.wrappedBuffer("testtest2".getBytes()));
        BridgeMessagePublish msg = new BridgeMessagePublish(Arrays.asList(mqttPub1, mqttPub2));

        // Encode bridge publish message
        BridgeEncoder encoder = new BridgeEncoder();
        encoder.encode(mockCtx, msg, mockOut);

        // Decode bridge publish message
        BridgeDecoder decoder = new BridgeDecoder();
        decoder.decode(mockCtx, (BridgeMessage) mockOut.get(0), resultOut);

        // Compare result of decoding with original message
        assertThat(msg).isEqualToComparingFieldByFieldRecursively(resultOut.get(0));
    }
}
