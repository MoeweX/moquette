package io.moquette.delaygrouping.peering.messaging;

import io.moquette.delaygrouping.SubscriptionStore;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;

public class PeerMessageMembership extends PeerMessage {
    private MembershipSignal signal;
    private int electionValue;
    private boolean shouldBeLeader;
    private InetAddress leavingPeer;
    private Collection<InetAddress> groupMembers;
    private SubscriptionStore groupSubscriptions;

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
        return joinAck(shouldBeLeader, new ArrayList<>(), new SubscriptionStore());
    }

    public static PeerMessageMembership joinAck(boolean shouldBeLeader, Collection<InetAddress> migratingMembers, SubscriptionStore groupSubscriptions) {
        var msg = new PeerMessageMembership(MembershipSignal.JOIN_ACK);
        msg.shouldBeLeader = shouldBeLeader;
        msg.groupMembers = new ArrayList<>(migratingMembers);
        msg.groupSubscriptions = new SubscriptionStore(groupSubscriptions);
        return msg;
    }

    public static PeerMessageMembership joinAckAck(PeerMessageMembership joinAck) {
        return joinAckAck(joinAck, new ArrayList<>(), new SubscriptionStore());
    }

    public static PeerMessageMembership joinAckAck(PeerMessageMembership joinAck, Collection<InetAddress> migratingMembers, SubscriptionStore groupSubscriptions) {
        var msg = new PeerMessageMembership(MembershipSignal.JOIN_ACKACK);
        // invert this as the receiver should do the exact opposite
        msg.shouldBeLeader = !joinAck.shouldBeLeader;
        msg.groupMembers = new ArrayList<>(migratingMembers);
        msg.groupSubscriptions = new SubscriptionStore(groupSubscriptions);
        return msg;
    }

    public static PeerMessageMembership leave(InetAddress leavingPeer) {
        var msg = new PeerMessageMembership(MembershipSignal.LEAVE);
        msg.leavingPeer = leavingPeer;
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
            case JOIN_ACKACK:
                return "PeerMessageMembership{" +
                    "signal=" + signal +
                    ", shouldBeLeader=" + shouldBeLeader +
                    ", groupMembers=" + groupMembers +
                    ", groupSubscriptions=" + groupSubscriptions +
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

    public Collection<InetAddress> getGroupMembers() {
        return groupMembers;
    }

    public SubscriptionStore getGroupSubscriptions() {
        return groupSubscriptions;
    }

    public enum MembershipSignal {
        JOIN(1),
        JOIN_ACK(2),
        JOIN_ACKACK(3),
        LEAVE(0);

        private final int sequenceNumber;

        MembershipSignal(int sequenceNumber) {
            this.sequenceNumber = sequenceNumber;
        }

        public int sequenceNumber() {
            return sequenceNumber;
        }
    }
}
