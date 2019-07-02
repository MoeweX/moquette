package io.moquette.delaygrouping.peering.messaging;

public class PeerMessageMembership extends PeerMessage {
    private MembershipSignal signal;
    private int electionValue;
    private boolean shouldBeLeader;

    private PeerMessageMembership(MembershipSignal signal) {
        super(PeerMessageType.MEMBERSHIP);
        this.signal = signal;
    }

    public MembershipSignal getSignal() {
        return signal;
    }

    public int getElectionValue() {
        return electionValue;
    }

    public static PeerMessageMembership join() {
        return new PeerMessageMembership(MembershipSignal.JOIN);
    }

    public static PeerMessageMembership joinAck() {
        return new PeerMessageMembership(MembershipSignal.JOIN_ACK);
    }

    public static PeerMessageMembership electionRequest(int electionValue) {
        var msg = new PeerMessageMembership(MembershipSignal.ELECTION_REQUEST);
        msg.electionValue = electionValue;
        return msg;
    }

    public static PeerMessageMembership electionResponse(boolean shouldBeLeader) {
        var msg = new PeerMessageMembership(MembershipSignal.ELECTION_RESPONSE);
        msg.shouldBeLeader = shouldBeLeader;
        return msg;
    }

    public enum MembershipSignal {
        JOIN,
        JOIN_ACK,
        ELECTION_REQUEST,
        ELECTION_RESPONSE,
    }
}
