package io.moquette.delaygrouping.anchor;

import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class MqttConnectionTest {

    @Test
    public void testPubSub() throws MqttException, InterruptedException {
        List<Message> receivedMessages1 = new ArrayList<>();
        List<Message> receivedMessages2 = new ArrayList<>();

        Message msg1 = new Message("test2", "This is on test2".getBytes(StandardCharsets.UTF_8));
        Message msg2 = new Message("test", "This is on test".getBytes(StandardCharsets.UTF_8));

        MqttConnection conn1 = new MqttConnection("tcp://localhost:1883", "conn1");
        MqttConnection conn2 = new MqttConnection("tcp://localhost:1883", "conn2");

        conn1.addMessageHandlerForTopic("test", receivedMessages1::add);
        conn2.addMessageHandlerForTopic("test2", receivedMessages2::add);

        conn1.addSubscription("test");
        conn2.addSubscription("test2");

        Thread.sleep(2000);

        conn1.publish(msg1);
        conn2.publish(msg2);

        Thread.sleep(2000);

        assertThat(receivedMessages1).asList().usingFieldByFieldElementComparator().containsExactlyInAnyOrder(msg2);
        assertThat(receivedMessages2).asList().usingFieldByFieldElementComparator().containsExactlyInAnyOrder(msg1);

        conn1.disconnect();
        conn2.disconnect();
    }
}
