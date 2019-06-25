package io.moquette.bridge.messaging;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.*;

import java.util.Random;

public class BridgeMessageFactory {

    public static MqttMessage connect(String bridgeId) {
        MqttFixedHeader fixedHeader = new MqttFixedHeader(
            MqttMessageType.CONNECT,
            false,
            MqttQoS.AT_LEAST_ONCE,
            false,
            0
        );
        MqttConnectVariableHeader connectVariableHeader = new MqttConnectVariableHeader(
            MqttVersion.MQTT_3_1_1.protocolName(),
            MqttVersion.MQTT_3_1_1.protocolLevel(),
            false,
            false,
            false,
            MqttQoS.AT_LEAST_ONCE.value(),
            false,
            true,
            2
        );
        MqttConnectPayload connectPayload = new MqttConnectPayload(
            bridgeId,
            "",
            "".getBytes(),
            "",
            "".getBytes()
        );

        return new MqttConnectMessage(fixedHeader, connectVariableHeader, connectPayload);
    }

    public static MqttMessage connAck(String bridgeId) {
        MqttFixedHeader fixedHeader = new MqttFixedHeader(
            MqttMessageType.CONNACK,
            false,
            MqttQoS.AT_LEAST_ONCE,
            false,
            0
        );
        MqttConnAckVariableHeader connAckVariableHeader = new MqttConnAckVariableHeader(
            MqttConnectReturnCode.CONNECTION_ACCEPTED,
            false
        );

        return new MqttConnAckMessage(fixedHeader, connAckVariableHeader);
    }

    public static MqttPublishMessage publish(String topicName, ByteBuf payload) {
        return publish(topicName, payload, (new Random()).nextInt(Integer.MAX_VALUE));
    }

    public static MqttPublishMessage publish(String topicName, ByteBuf payload, int packetId) {
        MqttFixedHeader fixedHeader = new MqttFixedHeader(
            MqttMessageType.PUBLISH,
            false,
            MqttQoS.AT_MOST_ONCE,
            false,
            0
        );
        MqttPublishVariableHeader variableHeader = new MqttPublishVariableHeader(topicName, packetId);
        return new MqttPublishMessage(fixedHeader, variableHeader, payload);
    }
}
