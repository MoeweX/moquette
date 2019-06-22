package io.moquette.delaygrouping.bridge.messaging;

public class BridgeMessageConnect extends BridgeMessage {

    public final String bridgeId;

    public BridgeMessageConnect(String bridgeId) {
        super(BridgeMessageType.CONNECT);
        this.bridgeId = bridgeId;
    }
}
