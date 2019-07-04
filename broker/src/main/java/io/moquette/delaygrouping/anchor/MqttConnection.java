package io.moquette.delaygrouping.anchor;

import org.eclipse.paho.client.mqttv3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class MqttConnection {
    private static final Logger LOG = LoggerFactory.getLogger(MqttConnection.class);

    private MqttClient mqttClient;
    private ExecutorService mqttExecutor = Executors.newSingleThreadExecutor();
    private ExecutorService resultExecutor = Executors.newSingleThreadExecutor();
    private LinkedBlockingDeque<Message> sendQueue = new LinkedBlockingDeque<>();
    private Set<String> subscribedTopics = ConcurrentHashMap.newKeySet();
    private Set<String> intendedTopics = ConcurrentHashMap.newKeySet();
    private Consumer<Message> messageHandler;
    private boolean connected;

    public MqttConnection(String serverURI, String clientId) throws MqttException {
        this.mqttClient = new MqttClient(serverURI, clientId, null);
        mqttClient.setCallback(createCallback(this::messageConsumer));
        mqttClient.connect(createConnectOptions());
        connected = true;

        var subscriptionMonitorExecutor = Executors.newSingleThreadExecutor();
        subscriptionMonitorExecutor.execute(() -> {
            while (connected) {
                var missingSubscriptions = new HashSet<>(intendedTopics);
                missingSubscriptions.removeAll(subscribedTopics);
                missingSubscriptions.forEach(topic -> {
                    try {
                        subscribe(topic).get();
                        subscribedTopics.add(topic);
                    } catch (ExecutionException | InterruptedException e) {
                        LOG.error("Error while subscribing to topic {}", topic, e);
                    }
                });

                var unwantedSubscriptions = new HashSet<>(subscribedTopics);
                unwantedSubscriptions.removeAll(intendedTopics);
                unwantedSubscriptions.forEach(topic -> {
                    try {
                        unsubscribe(topic).get();
                        subscribedTopics.remove(topic);
                    } catch (InterruptedException | ExecutionException e) {
                        LOG.error("Error while unsubscribing from topic {}", topic, e);
                    }
                });

                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        var sendQueueExecutor = Executors.newSingleThreadExecutor();
        sendQueueExecutor.execute(() -> {
            while (connected) {
                try {
                    var msg = sendQueue.take();
                    mqttExecutor.execute(() -> {
                        try {
                            mqttClient.publish(msg.topic, msg.payload, 0, false);
                        } catch (MqttException e) {
                            e.printStackTrace();
                            sendQueue.addFirst(msg);
                        }
                    });
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    private static MqttConnectOptions createConnectOptions() {
        var options = new MqttConnectOptions();
        options.setAutomaticReconnect(false);
        options.setCleanSession(true);
        options.setMqttVersion(MqttConnectOptions.MQTT_VERSION_3_1_1);
        return options;
    }

    public void setMessageHandler(Consumer<Message> handler) {
        messageHandler = handler;
    }

    private Future subscribe(String topic) {
        var future = new CompletableFuture<>();
        mqttExecutor.execute(() -> {
            try {
                mqttClient.subscribe(topic, 0);
                future.complete(null);
            } catch (MqttException e) {
                LOG.error("Error while subscribing for topic {} on {}: {}", topic, mqttClient.getServerURI(), e);
                future.completeExceptionally(e);
            }
        });
        return future;
    }

    private Future unsubscribe(String topic) {
        var future = new CompletableFuture<>();
        mqttExecutor.execute(() -> {
            try {
                mqttClient.unsubscribe(topic);
                future.complete(null);
            } catch (MqttException e) {
                LOG.error("Error while subscribing for topic {} on {}: {}", topic, mqttClient.getServerURI(), e);
                future.completeExceptionally(e);
            }
        });
        return future;
    }

    public void addSubscription(String topic) {
        intendedTopics.add(topic);
    }

    public void removeSubscription(String topic) {
        intendedTopics.remove(topic);
    }

    public void publish(Message msg) {
        sendQueue.add(msg);
    }

    public void disconnect() {
        mqttExecutor.execute(() -> {
            try {
                mqttClient.disconnect();
                mqttClient.close();
                connected = false;
            } catch (MqttException e) {
                e.printStackTrace();
            }
        });
    }

    private void messageConsumer(String topic, MqttMessage msg) {
        var message = new Message(topic, msg.getPayload());

        resultExecutor.execute(() -> messageHandler.accept(message));
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
