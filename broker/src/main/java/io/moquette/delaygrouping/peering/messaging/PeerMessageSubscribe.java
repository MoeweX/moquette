package io.moquette.delaygrouping.peering.messaging;

public class PeerMessageSubscribe extends PeerMessage {
    private String topicFilter;

    private PeerMessageSubscribe() {
        super(PeerMessageType.SUBSCRIBE);
    }

    public static PeerMessageSubscribe fromTopicFilter(String topicFilter) {
        var msg = new PeerMessageSubscribe();
        msg.topicFilter = topicFilter;
        return msg;
    }

    public String getTopicFilter() {
        return topicFilter;
    }
}
