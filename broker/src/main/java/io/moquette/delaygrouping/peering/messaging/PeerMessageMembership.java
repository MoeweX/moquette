package io.moquette.delaygrouping.peering.messaging;

public class PeerMessageMembership extends PeerMessage {
    private MembershipSignal signal;

    public PeerMessageMembership(MembershipSignal signal) {
        super(PeerMessageType.MEMBERSHIP);
        this.signal = signal;
    }

    public MembershipSignal getSignal() {
        return signal;
    }

    public enum MembershipSignal {
        JOIN,
        JOIN_ACK,
        LEAVE
    }
}
