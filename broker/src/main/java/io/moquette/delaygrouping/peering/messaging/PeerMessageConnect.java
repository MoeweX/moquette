package io.moquette.delaygrouping.peering.messaging;

public class PeerMessageConnect extends PeerMessage {

    public final String peerId;

    public PeerMessageConnect(String peerId) {
        super(PeerMessageType.CONNECT);
        this.peerId = peerId;
    }
}
