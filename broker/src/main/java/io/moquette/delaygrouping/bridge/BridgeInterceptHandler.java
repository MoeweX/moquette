package io.moquette.delaygrouping.bridge;

import io.moquette.delaygrouping.bridge.messaging.BridgeMessagePublish;
import io.moquette.interception.AbstractInterceptHandler;
import io.moquette.interception.messages.InterceptPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.Arrays;

public class BridgeInterceptHandler extends AbstractInterceptHandler {

    private static final Logger LOG = LoggerFactory.getLogger(BridgeConnection.class);
    private Bridge bridge;

    public BridgeInterceptHandler(Bridge bridge) {
        this.bridge = bridge;
    }

    @Override
    public Class<?>[] getInterceptedMessageTypes() {
        return new Class[]{InterceptPublishMessage.class};
    }

    @Override
    public void onPublish(InterceptPublishMessage msg) {
        try {
            // Hacky as f**k...
            Field pubMsgField = msg.getClass().getDeclaredField("msg");
            pubMsgField.setAccessible(true);
            MqttPublishMessage pubMsg = (MqttPublishMessage) pubMsgField.get(msg);

            BridgeMessagePublish publishMessage = new BridgeMessagePublish(Arrays.asList(pubMsg));
            bridge.bridgePublish(publishMessage);
        } catch (NoSuchFieldException | IllegalAccessException ex) {
            LOG.error("Error while retrieving MQTT messages from intercepted message.", ex);
        }

    }

    @Override
    public String getID() {
        return "BridgeInterceptHandler";
    }
}
