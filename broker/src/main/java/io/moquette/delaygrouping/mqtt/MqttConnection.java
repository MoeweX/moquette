package io.moquette.delaygrouping.mqtt;

import org.eclipse.paho.client.mqttv3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class MqttConnection {
    private static final Logger LOG = LoggerFactory.getLogger(MqttConnection.class);

    private Consumer<Message> messageHandler;
    private MqttClient mqttClient;
    private ExecutorService mqttExecutor = Executors.newSingleThreadExecutor();
    private ExecutorService resultExecutor = Executors.newSingleThreadExecutor();

    public MqttConnection(String serverURI, String clientId) throws MqttException {
        this.mqttClient = new MqttClient(serverURI, clientId, null);
        mqttClient.setCallback(createCallback(this::messageConsumer));
        try {
            mqttClient.connect(createConnectOptions());
        } catch (MqttException e) {
            LOG.error("MQTT connection to {} failed: {}", serverURI, e);
            throw e;
        }
    }

    private static MqttConnectOptions createConnectOptions() {
        MqttConnectOptions options = new MqttConnectOptions();
        options.setAutomaticReconnect(false);
        options.setCleanSession(true);
        options.setMqttVersion(MqttConnectOptions.MQTT_VERSION_3_1_1);
        return options;
    }

    public void setMessageHandler(Consumer<Message> handler) {
        this.messageHandler = handler;
    }

    public void addSubscription(String topic) {
        mqttExecutor.execute(() -> {
            try {
                mqttClient.subscribe(topic);
            } catch (MqttException e) {
                LOG.error("Error while subscribing for topic {} on {}: {}", topic, mqttClient.getServerURI(), e);
            }
        });
    }

    public void publish(Message msg) {
        mqttExecutor.execute(() -> {
            try {
                mqttClient.publish(msg.topic, msg.payload, 0, false);
            } catch (MqttException e) {
                LOG.error("Error while publishing to {}: {}", mqttClient.getServerURI(), e);
            }
        });
    }

    public void disconnect() {
        try {
            mqttExecutor.submit(() -> {
                try {
                    mqttClient.disconnectForcibly();
                    mqttClient.close();
                } catch (MqttException e) {
                    e.printStackTrace();
                }
            }).get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    private void messageConsumer(String topic, MqttMessage msg) {
        if (messageHandler != null) {
            resultExecutor.execute(() -> messageHandler.accept(new Message(topic, msg.getPayload())));
        }

        // Add to queue?
    }

    private MqttCallback createCallback(BiConsumer<String, MqttMessage> consumer) {
        return new MqttCallback() {
            @Override
            public void connectionLost(Throwable cause) {

            }

            @Override
            public void messageArrived(String topic, MqttMessage message) {
                consumer.accept(topic, message);
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken token) {

            }
        };
    }

}
