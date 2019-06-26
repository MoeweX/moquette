package io.moquette.delaygrouping;

public class Orchestrator {

    public Orchestrator() {
        // We need some config: anchorNode, latencyThreshold
        // This also needs some state, determining which mode we're in (LEADER, NON_LEADER, etc.?)

        // Enter LEADER mode
        // Connect to anchor node
        // TODO Should we already accept client connections?
        // Do an MQTT SUB on the /$SYS/COLLIN/leaders topic

        // Build a list of other leaders
        // Add them to monitoring
        // Check if any other leader is below latencyThreshold

        // Connect to the leader with the lowest delay
        // "Hello, I'd like to join your group and I do want to be the leader / I don't want to be a leader"
        // Do leader election between the two
        // Either doLeader or doNonLeader as a result

        // TODO What if another node joins while two are already negotiating? Put on hold for now? Redirect to new leader?
        // TODO How about all nodes can redirect to their leader?

    }

    private void doLeader() {
        // Publish IP on /$SYS/COLLIN/leaders topic
        // Check other leaders for nodes below the threshold

        // Collect subscriptions and forward them to cloud anchor

        // Forward matching PUBs from cloud anchor to group members

        // Forward all PUBs from group members to cloud
    }

    private void doNonLeader() {
        //
    }
}
