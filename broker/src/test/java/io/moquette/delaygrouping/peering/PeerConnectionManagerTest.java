package io.moquette.delaygrouping.peering;

import io.moquette.delaygrouping.peering.messaging.PeerMessage;
import io.moquette.delaygrouping.peering.messaging.PeerMessageRedirect;
import io.moquette.delaygrouping.peering.messaging.PeerMessageType;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;

import static org.assertj.core.api.Assertions.assertThat;

public class PeerConnectionManagerTest {
    @Test
    public void testConnectionTwoInstances() throws UnknownHostException, InterruptedException {
        var rcvMessages = new LinkedBlockingQueue<PeerMessage>();

        var instance1 = new PeerConnectionManager(InetAddress.getByName("127.0.0.1"));
        var instance2 = new PeerConnectionManager(InetAddress.getByName("127.0.0.2"));

        instance1.getConnectionToPeer(InetAddress.getByName("127.0.0.2"))
            .registerMessageHandler(rcvMessages::add, PeerMessageType.REDIRECT);

        Thread.sleep(1000);

        instance2.getConnectionToPeer(InetAddress.getByName("127.0.0.1"))
            .registerMessageHandler(rcvMessages::add, PeerMessageType.REDIRECT);

        Thread.sleep(2000);

        var msg1 = new PeerMessageRedirect(InetAddress.getByName("google.de"));
        instance1.getConnectionToPeer(InetAddress.getByName("127.0.0.2"))
            .sendMessage(msg1);

        var msg2 = new PeerMessageRedirect(InetAddress.getByName("heise.de"));
        instance2.getConnectionToPeer(InetAddress.getByName("127.0.0.1"))
            .sendMessage(msg2);

        Thread.sleep(3000);

        var rcvMsgList = new ArrayList<PeerMessage>();
        rcvMessages.drainTo(rcvMsgList);
        assertThat(rcvMsgList).asList().usingFieldByFieldElementComparator().containsExactlyInAnyOrder(msg1, msg2);
    }

    @Test
    public void testAddAndRemoveConnections() {

    }
}
