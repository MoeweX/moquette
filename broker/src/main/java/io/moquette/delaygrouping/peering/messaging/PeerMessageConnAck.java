package io.moquette.delaygrouping.peering.messaging;

public class PeerMessageConnAck extends PeerMessage {

    public final String peerId;

    public PeerMessageConnAck(String peerId) {
        super(PeerMessageType.CONNACK);
        this.peerId = peerId;
    }
}
