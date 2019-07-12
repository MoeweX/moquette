package io.moquette.delaygrouping;

import io.moquette.delaygrouping.peering.messaging.PeerMessageMembership;
import io.moquette.delaygrouping.peering.messaging.PeerMessageMembership.MembershipSignal;

import java.net.InetAddress;

public class MembershipTransaction {
    private static int EXPIRATION_TIME = 1000;
    private InetAddress peerAddress;
    private MembershipSignal lastSignal;
    private long startTimestamp;

    MembershipTransaction(InetAddress peerAddress) {
        this.peerAddress = peerAddress;
        this.startTimestamp = System.nanoTime();
    }

    public boolean commitMessage(PeerMessageMembership msg) {
        var signal = msg.getSignal();
        if (lastSignal == null) {
            lastSignal = signal;
            return true;
        }
        if (signal.equals(MembershipSignal.JOIN) ||
            signal.equals(MembershipSignal.JOIN_ACK) ||
            signal.equals(MembershipSignal.JOIN_ACKACK)) {
            if (lastSignal.sequenceNumber() + 1 == signal.sequenceNumber()) {
                lastSignal = signal;
                return true;
            } else {
                return false;
            }
        } else {
            // TODO Always allow leave?
            return true;
        }
    }

    public boolean hasExpired() {
        return System.nanoTime() - startTimestamp > EXPIRATION_TIME * 1_000_000 || lastSignal.equals(MembershipSignal.JOIN_ACKACK);
    }

    public MembershipSignal getLastSignal() {
        return lastSignal;
    }

    public InetAddress getPeerAddress() {
        return peerAddress;
    }
}
