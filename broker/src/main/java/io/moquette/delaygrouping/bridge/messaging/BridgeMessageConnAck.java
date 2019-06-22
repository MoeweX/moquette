package io.moquette.delaygrouping.bridge.messaging;

public class BridgeMessageConnAck extends BridgeMessage {

    public final String bridgeId;

    public BridgeMessageConnAck(String bridgeId) {
        super(BridgeMessageType.CONNACK);
        this.bridgeId = bridgeId;
    }
}
