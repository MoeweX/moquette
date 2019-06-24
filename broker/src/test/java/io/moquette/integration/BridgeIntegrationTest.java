package io.moquette.integration;

import io.moquette.server.MessageCollector;
import io.moquette.server.Server;
import io.moquette.server.config.IConfig;
import io.moquette.server.config.MemoryConfig;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.Charset;
import java.time.ZonedDateTime;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

public class BridgeIntegrationTest {


    private List<Server> testInstances = new ArrayList<>();
    private List<MqttClient> testClients = new ArrayList<>();
    private Map<MqttClient, MessageCollector> msgCollectors = new HashMap<>();

    private Server runTestInstance(String port, String bridge_port, String bridge_connections) {
        try {
            Server testInstance = new Server();
            final Properties configProps = new Properties();
            configProps.setProperty("port", port);
            configProps.setProperty("host", "127.0.0.1");
            configProps.setProperty("bridge_port", bridge_port);
            configProps.setProperty("bridge_host", "127.0.0.1");
            configProps.setProperty("bridge_connections", bridge_connections);

            IConfig config = new MemoryConfig(configProps);
            testInstance.startServer(config);

            testInstances.add(testInstance);
            return testInstance;
        } catch (IOException e) {
            return null;
        }
    }

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
        System.out.println("Tearing down infrastructure");
        for (MqttClient client : testClients) {
            if (client.isConnected()) client.disconnect();
        }
        testClients.clear();
        for (Server server : testInstances) {
            server.stopServer();
        }
        testInstances.clear();
    }

    @Test
    public void testBridgePublish2Bidirectional() throws MqttException, InterruptedException {
        runTestInstance("1883", "1884", "127.0.0.1:2884");
        runTestInstance("2883", "2884", "127.0.0.1:1884");
        MqttClient testClient1 = getTestClient("tcp://localhost:1883", "testClient1");
        MqttClient testClient2 = getTestClient("tcp://localhost:2883", "testClient2");

        Thread.sleep(2000);

        MqttMessage testMsg = new MqttMessage("testtest".getBytes(Charset.forName("UTF-8")));

        testClient1.connect();
        testClient1.subscribe("test", 0);

        testClient2.connect();
        testClient2.publish("test", testMsg);

        MqttMessage rcvMsg = msgCollectors.get(testClient1).waitMessage(4);

        assertThat(rcvMsg).isNotNull();
        assertThat(rcvMsg).isEqualToComparingOnlyGivenFields(testMsg, "messageId", "payload");
    }

    @Test
    public void testBridgePublishRing3Bidirectional() throws MqttException, InterruptedException {
        runTestInstance("1883", "1884", "127.0.0.1:2884, 127.0.0.1:3884");
        runTestInstance("2883", "2884", "127.0.0.1:1884, 127.0.0.1:3884");
        runTestInstance("3883", "3884", "127.0.0.1:1884, 127.0.0.1:2884");
        MqttClient testClient1 = getTestClient("tcp://localhost:1883", "testClient1");
        MqttClient testClient2 = getTestClient("tcp://localhost:2883", "testClient2");
        MqttClient testClient3 = getTestClient("tcp://localhost:3883", "testClient3");

        Thread.sleep(5000);

        MqttMessage testMsg = new MqttMessage("testtest".getBytes(Charset.forName("UTF-8")));

        testClient1.connect();
        testClient1.subscribe("test", 0);

        testClient3.connect();
        testClient3.subscribe("test", 0);

        testClient2.connect();
        testClient2.publish("test", testMsg);

        MqttMessage rcvMsg1 = null;
        MqttMessage rcvMsg3 = null;
        while (rcvMsg1 == null) {
            rcvMsg1 = msgCollectors.get(testClient1).waitMessage(2);
        }
        while (rcvMsg3 == null) {
            rcvMsg3 = msgCollectors.get(testClient3).waitMessage(2);
        }

        assertThat(rcvMsg1).isEqualToComparingOnlyGivenFields(testMsg, "messageId", "payload");
        assertThat(rcvMsg3).isEqualToComparingOnlyGivenFields(testMsg, "messageId", "payload");
    }

    @Test
    @Ignore
    public void testBridgePublishWithLocalReceiver() throws MqttException, InterruptedException {
        runTestInstance("1883", "1884", "127.0.0.1:2884, 127.0.0.1:3884");
        runTestInstance("2883", "2884", "127.0.0.1:1884, 127.0.0.1:3884");
        runTestInstance("3883", "3884", "127.0.0.1:1884, 127.0.0.1:2884");
        MqttClient testClient1 = getTestClient("tcp://localhost:1883", "testClient1");
        MqttClient testClient11 = getTestClient("tcp://localhost:1883", "testClient11");
        MqttClient testClient2 = getTestClient("tcp://localhost:2883", "testClient2");
        MqttClient testClient3 = getTestClient("tcp://localhost:3883", "testClient3");

        Thread.sleep(10000);

        MqttMessage testMsg = new MqttMessage("testtest".getBytes(Charset.forName("UTF-8")));

        testClient1.connect();
        testClient1.subscribe("test", 0);
        testClient2.connect();
        testClient2.subscribe("test", 0);
        testClient3.connect();
        testClient3.subscribe("test", 0);

        testClient11.connect();
        testClient11.publish("test", testMsg);

        Map<String, MqttMessage> receivedMessages = new HashMap<>();
        long startTime = ZonedDateTime.now().toInstant().toEpochMilli();
        long currentTime;
        MqttMessage rcvMsg;
        do {
            currentTime = ZonedDateTime.now().toInstant().toEpochMilli();
            rcvMsg = msgCollectors.get(testClient1).waitMessage(2);
            if (rcvMsg != null) receivedMessages.put("testClient1_" + currentTime, rcvMsg);
            rcvMsg = msgCollectors.get(testClient2).waitMessage(2);
            if (rcvMsg != null) receivedMessages.put("testClient2_" + currentTime, rcvMsg);
            rcvMsg = msgCollectors.get(testClient3).waitMessage(2);
            if (rcvMsg != null) receivedMessages.put("testClient3_" + currentTime, rcvMsg);
        } while (currentTime - startTime < 10000);

        System.out.println(receivedMessages);
    }

    @Test
    public void manualTest1() throws InterruptedException {
        runTestInstance("1883", "1884", "127.0.0.1:2884");

        while(true) {
            Thread.sleep(1000);
        }
    }

    @Test
    public void manualTest2() throws InterruptedException {
        runTestInstance("2883", "2884", "127.0.0.1:1884");

        while(true) {
            Thread.sleep(1000);
        }
    }
}
