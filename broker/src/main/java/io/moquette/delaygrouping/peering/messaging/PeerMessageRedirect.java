package io.moquette.delaygrouping.peering.messaging;

import java.net.InetAddress;

public class PeerMessageRedirect extends PeerMessage {
    private InetAddress target;

    public PeerMessageRedirect(InetAddress target) {
        super(PeerMessageType.REDIRECT);
        this.target = target;
    }

    public InetAddress getTarget() {
        return target;
    }
}
