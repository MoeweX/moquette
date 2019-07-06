package io.moquette.integration;


import io.moquette.server.MessageCollector;
import io.moquette.server.Server;
import io.moquette.server.config.IConfig;
import io.moquette.server.config.MemoryConfig;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

public class DelaygroupingOrchestratorTest {

    private List<Server> testInstances = new ArrayList<>();
    private List<MqttClient> testClients = new ArrayList<>();
    private Map<MqttClient, MessageCollector> msgCollectors = new HashMap<>();

    private Server runTestInstance(String host) {
        try {
            Server testInstance = new Server();
            final Properties configProps = new Properties();
            configProps.setProperty("port", "1883");
            configProps.setProperty("host", host);
            configProps.setProperty("delaygrouping_peering_host", host);
            configProps.setProperty("delaygrouping_peering_port", "1884");
            configProps.setProperty("delaygrouping_threshold", "5");
            configProps.setProperty("delaygrouping_anchor_node_address", "127.0.0.5:1883");

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
    public void firstTest() throws InterruptedException {
        var instance1 = runTestInstance("127.0.0.1");

        Thread.sleep(5000);

        //var instance2 = runTestInstance("127.0.0.2");

        while (true) {
            Thread.sleep(1000);
        }
    }
}
