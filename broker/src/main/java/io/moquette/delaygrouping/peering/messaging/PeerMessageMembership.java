package io.moquette.delaygrouping.peering.messaging;

public class PeerMessageMembership extends PeerMessage {
    private MembershipSignal signal;
    private int electionValue;
    private boolean shouldBeLeader;

    private PeerMessageMembership(MembershipSignal signal) {
        super(PeerMessageType.MEMBERSHIP);
        this.signal = signal;
    }

    public static PeerMessageMembership join(int electionValue) {
        var msg = new PeerMessageMembership(MembershipSignal.JOIN);
        msg.electionValue = electionValue;
        return msg;
    }

    public static PeerMessageMembership joinAck(boolean shouldBeLeader) {
        var msg = new PeerMessageMembership(MembershipSignal.JOIN_ACK);
        msg.shouldBeLeader = shouldBeLeader;
        return msg;
    }

    public static PeerMessageMembership deny() {
        return new PeerMessageMembership(MembershipSignal.DENY);
    }

    public MembershipSignal getSignal() {
        return signal;
    }

    public int getElectionValue() {
        return electionValue;
    }

    public boolean isShouldBeLeader() {
        return shouldBeLeader;
    }

    public enum MembershipSignal {
        JOIN,
        JOIN_ACK,
        DENY,
    }
}
