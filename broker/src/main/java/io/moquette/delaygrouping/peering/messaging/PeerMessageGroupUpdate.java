package io.moquette.delaygrouping.peering.messaging;

import io.moquette.delaygrouping.SubscriptionStore;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;

public class PeerMessageGroupUpdate extends PeerMessage {
    private Collection<InetAddress> groupMembers;
    private SubscriptionStore groupSubscriptions;

    private PeerMessageGroupUpdate() {
        super(PeerMessageType.GROUP);
    }

    public static PeerMessageGroupUpdate update(Collection<InetAddress> groupMembers, SubscriptionStore groupSubscriptions) {
        var msg = new PeerMessageGroupUpdate();
        msg.groupMembers = new ArrayList<>(groupMembers);
        msg.groupSubscriptions = groupSubscriptions;
        return msg;
    }

    public Collection<InetAddress> getGroupMembers() {
        return groupMembers;
    }

    public SubscriptionStore getGroupSubscriptions() {
        return groupSubscriptions;
    }
}
