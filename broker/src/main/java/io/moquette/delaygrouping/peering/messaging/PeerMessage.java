package io.moquette.delaygrouping.peering.messaging;

import java.io.Serializable;
import java.util.UUID;

public class PeerMessage implements Serializable {

    private static final long serialVersionUID = 1L;

    public final PeerMessageType type;
    public final String uid = UUID.randomUUID().toString();

    public PeerMessage(PeerMessageType type) {
        this.type = type;
    }

    public void retain() {

    }

    @Override
    public String toString() {
        return "PeerMessage{" +
            "type=" + type +
            '}';
    }
}
