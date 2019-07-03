package io.moquette.delaygrouping.peering.messaging;

public class PeerMessageSubscribe extends PeerMessage {
    private String topicFilter;

    public PeerMessageSubscribe(String topicFilter) {
        super(PeerMessageType.SUBSCRIBE);
        this.topicFilter = topicFilter;
    }

    public String getTopicFilter() {
        return topicFilter;
    }
}
