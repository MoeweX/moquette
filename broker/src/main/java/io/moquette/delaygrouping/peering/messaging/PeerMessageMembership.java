package io.moquette.delaygrouping.peering.messaging;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;

public class PeerMessageMembership extends PeerMessage {
    private MembershipSignal signal;
    private int electionValue;
    private boolean shouldBeLeader;
    private InetAddress leavingPeer;
    private Collection<InetAddress> addedPeers;
    private Collection<InetAddress> removedPeers;
    private Collection<InetAddress> groupMembers;

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

    public static PeerMessageMembership joinAckAck(PeerMessageMembership joinAck) {
        var msg = new PeerMessageMembership(MembershipSignal.JOIN_ACKACK);
        // invert this as the receiver should do the exact opposite
        msg.shouldBeLeader = !joinAck.shouldBeLeader;
        return msg;
    }

    public static PeerMessageMembership deny() {
        return new PeerMessageMembership(MembershipSignal.DENY);
    }

    public static PeerMessageMembership busy() {
        return new PeerMessageMembership(MembershipSignal.BUSY);
    }

    public static PeerMessageMembership leave(InetAddress leavingPeer) {
        var msg = new PeerMessageMembership(MembershipSignal.LEAVE);
        msg.leavingPeer = leavingPeer;
        return msg;
    }

    public static PeerMessageMembership groupUpdate(Collection<InetAddress> addedPeers, Collection<InetAddress> removedPeers) {
        var msg = new PeerMessageMembership(MembershipSignal.GROUP_UPDATE);
        msg.addedPeers = Objects.requireNonNullElseGet(addedPeers, ArrayList::new);
        msg.removedPeers = Objects.requireNonNullElseGet(removedPeers, ArrayList::new);
        return msg;
    }

    public static PeerMessageMembership groupSet(Collection<InetAddress> groupMembers) {
        var msg = new PeerMessageMembership(MembershipSignal.GROUP_SET);
        msg.groupMembers = groupMembers;
        return msg;
    }

    @Override
    public String toString() {
        switch (signal) {
            case JOIN:
                return "PeerMessageMembership{" +
                    "signal=" + signal +
                    ", electionValue=" + electionValue +
                    '}';
            case JOIN_ACK:
                return "PeerMessageMembership{" +
                    "signal=" + signal +
                    ", shouldBeLeader=" + shouldBeLeader +
                    '}';
            case JOIN_ACKACK:
                return "PeerMessageMembership{" +
                    "signal=" + signal +
                    ", shouldBeLeader=" + shouldBeLeader +
                    '}';
            case GROUP_UPDATE:
                return "PeerMessageMembership{" +
                    "signal=" + signal +
                    ", addedPeers=" + addedPeers +
                    ", removedPeers=" + removedPeers +
                    '}';
            case GROUP_SET:
                return "PeerMessageMembership{" +
                    "signal=" + signal +
                    ", groupMembers=" + groupMembers +
                    '}';
            case LEAVE:
                return "PeerMessageMembership{" +
                    "signal=" + signal +
                    ", leavingPeer=" + leavingPeer +
                    '}';
            default:
                return "PeerMessageMembership{" +
                    "signal=" + signal +
                    '}';
        }

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

    public Collection<InetAddress> getAddedPeers() {
        return addedPeers;
    }

    public Collection<InetAddress> getRemovedPeers() {
        return removedPeers;
    }

    public Collection<InetAddress> getGroupMembers() {
        return groupMembers;
    }

    public enum MembershipSignal {
        JOIN,
        JOIN_ACK,
        JOIN_ACKACK,
        DENY,
        BUSY,
        LEAVE,
        GROUP_UPDATE,
        GROUP_SET,
    }
}
