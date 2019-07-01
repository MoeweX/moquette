package io.moquette.delaygrouping.anchor;


import io.moquette.server.MessageCollector;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.After;
import org.junit.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import static org.assertj.core.api.Assertions.assertThat;

public class AnchorConnectionTest {

    private List<MqttClient> testClients = new ArrayList<>();
    private Map<MqttClient, MessageCollector> msgCollectors = new HashMap<>();

    private MqttClient getTestClient(String url, String clientId) throws MqttException {
        MqttClient testClient = new MqttClient(url, clientId, null);
        MessageCollector msgCollector = new MessageCollector();
        testClient.setCallback(msgCollector);
        testClients.add(testClient);
        msgCollectors.put(testClient, msgCollector);
        return testClient;
    }

    @After
    public void tearDown() throws MqttException {
        for (MqttClient client : testClients) {
            if (client.isConnected()) client.disconnect();
        }
        testClients.clear();
    }

    @Test
    public void leaderAnnouncement() throws MqttException, UnknownHostException, InterruptedException {
        // Start local mosquitto instance for that
        var anchorConnection1 = new AnchorConnection(new InetSocketAddress("127.0.0.1", 1883), InetAddress.getByName("127.0.0.1"));
        var anchorConnection2 = new AnchorConnection(new InetSocketAddress("127.0.0.1", 1883), InetAddress.getByName("127.0.0.2"));

        anchorConnection1.startLeaderAnnouncement();
        anchorConnection2.startLeaderAnnouncement();

        Thread.sleep(2000);

        assertThat(anchorConnection1.getCollectedLeaders()).containsExactlyInAnyOrder(InetAddress.getByName("127.0.0.2"));
        assertThat(anchorConnection2.getCollectedLeaders()).containsExactlyInAnyOrder(InetAddress.getByName("127.0.0.1"));
        anchorConnection1.stopLeaderAnnouncement();

        Thread.sleep(2000);

        assertThat(anchorConnection1.getCollectedLeaders()).isEmpty();
        assertThat(anchorConnection2.getCollectedLeaders()).isEmpty();
    }

    public void mqttCommunication() throws MqttException, InterruptedException, UnknownHostException {
        var subscriber = getTestClient("tcp://localhost:1883", "testSubscriber");
        var publisher = getTestClient("tcp://localhost:1883", "testPublisher");

        var anchorConnection1 = new AnchorConnection(new InetSocketAddress("127.0.0.1", 1883), InetAddress.getByName("127.0.0.1"));
        var anchorConnection2 = new AnchorConnection(new InetSocketAddress("127.0.0.1", 1883), InetAddress.getByName("127.0.0.2"));

        var receivedMessageQueue1 = new LinkedBlockingQueue<Message>();
        var receivedMessageQueue2 = new LinkedBlockingQueue<Message>();

        anchorConnection1.setMqttCallback(receivedMessageQueue1::add);
        anchorConnection1.startLeaderAnnouncement();

        anchorConnection2.setMqttCallback(receivedMessageQueue2::add);
        anchorConnection2.startLeaderAnnouncement();

        Thread.sleep(2000);


    }
}
