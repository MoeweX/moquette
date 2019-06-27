package io.moquette.delaygrouping.peering;

import io.moquette.delaygrouping.peering.messaging.PeerMessage;
import io.moquette.delaygrouping.peering.messaging.PeerMessageType;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public class PeerConnection {

    private Map<String, Consumer<PeerMessage>> handlers = new ConcurrentHashMap<>();
    private Map<PeerMessageType, List<Consumer<PeerMessage>>> handlersByType = new ConcurrentHashMap<>();

    // TODO First draft of the API (still need to make that fit into the Netty architecture)

    public PeerConnection(InetAddress peerAddress) {
        // Create a new connection to the given peer and try to establish a connection
        // Assume it'll work (as we assume full overlay connectivity anyway)

        // Initialize handlers by type
        for (PeerMessageType type : PeerMessageType.values()) {
            handlersByType.put(type, new ArrayList<>());
        }
    }

    public void sendMessage(PeerMessage msg) {
        // send a message to the peer
    }

    public String registerMessageHandler(Consumer<PeerMessage> handler, PeerMessageType... messageTypes) {
        // listen for a specific message type (in order to isolate or react to a certain type of communication)
        // there can be multiple overlapping handlers
        // What about threading? Call each on a separate thread?
        String handlerId = UUID.randomUUID().toString();
        handlers.put(handlerId, handler);
        for (PeerMessageType type : messageTypes) {
            handlersByType.get(type).add(handler);
        }
        return handlerId;
    }

    public void removeMessageHandler(String handlerId) {
        Consumer<PeerMessage> handler = handlers.remove(handlerId);
        if (handler != null) {
            for (PeerMessageType type : PeerMessageType.values()) {
                handlersByType.get(type).remove(handler);
            }
        }
    }
}
