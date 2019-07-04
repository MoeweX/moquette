package io.moquette.delaygrouping.peering.messaging;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class PeerMessageMembership extends PeerMessage {
    private MembershipSignal signal;
    private int electionValue;
    private boolean shouldBeLeader;
    private InetAddress leavingPeer;
    private List<InetAddress> leftPeers;
    private List<InetAddress> joinedPeers;

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

    public static PeerMessageMembership leave(InetAddress leavingPeer) {
        var msg = new PeerMessageMembership(MembershipSignal.LEAVE);
        msg.leavingPeer = leavingPeer;
        return msg;
    }

    public static PeerMessageMembership groupUpdate(List<InetAddress> leftPeers, List<InetAddress> joinedPeers) {
        var msg = new PeerMessageMembership(MembershipSignal.GROUP_UPDATE);
        msg.leftPeers = Objects.requireNonNullElseGet(leftPeers, ArrayList::new);
        msg.joinedPeers = Objects.requireNonNullElseGet(joinedPeers, ArrayList::new);
        return msg;
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

    public InetAddress getLeavingPeer() {
        return leavingPeer;
    }

    public List<InetAddress> getLeftPeers() {
        return leftPeers;
    }

    public List<InetAddress> getJoinedPeers() {
        return joinedPeers;
    }

    public enum MembershipSignal {
        JOIN,
        JOIN_ACK,
        DENY,
        LEAVE,
        GROUP_UPDATE,
    }
}
