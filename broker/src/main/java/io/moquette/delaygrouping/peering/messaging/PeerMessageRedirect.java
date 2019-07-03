package io.moquette.delaygrouping.peering.messaging;

import java.net.InetAddress;

public class PeerMessageRedirect extends PeerMessage {
    private InetAddress target;

    private PeerMessageRedirect() {
        super(PeerMessageType.REDIRECT);
    }

    public InetAddress getTarget() {
        return target;
    }

    public static PeerMessageRedirect redirect(InetAddress target) {
        var message = new PeerMessageRedirect();
        message.target = target;
        return message;
    }
}
