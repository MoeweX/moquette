package io.moquette.bridge.messaging;

import java.io.Serializable;
import java.util.UUID;

public class BridgeMessage implements Serializable {

    private static final long serialVersionUID = 1L;

    public final BridgeMessageType type;
    public final String uid = UUID.randomUUID().toString();

    public BridgeMessage(BridgeMessageType type) {
        this.type = type;
    }
}
