package io.moquette.delaygrouping.peering.messaging;

import java.util.Collection;

public class PeerMessageSubscribe extends PeerMessage {
    private Collection<String> topicFilters;

    private PeerMessageSubscribe() {
        super(PeerMessageType.SUBSCRIBE);
    }

    public static PeerMessageSubscribe fromTopicFilter(Collection<String> topicFilters) {
        var msg = new PeerMessageSubscribe();
        msg.topicFilters = topicFilters;
        return msg;
    }

    public Collection<String> getTopicFilters() {
        return topicFilters;
    }
}
