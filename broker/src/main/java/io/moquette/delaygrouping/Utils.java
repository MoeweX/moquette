package io.moquette.delaygrouping;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.MqttMessageBuilders;
import io.netty.handler.codec.mqtt.MqttMessageFactory;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Collections;
import java.util.Optional;
import java.util.Random;
import java.util.regex.Pattern;

public class Utils {
    public static Optional<InetAddress> getInterfaceIpv4Address(String interfaceName) {
        try {
            var networkInterfaces = Collections.list(NetworkInterface.getNetworkInterfaces());
            var networkInterface = networkInterfaces.stream().filter(i -> i.getName().equals(interfaceName)).findFirst();
            if (networkInterface.isPresent()) {
                var interfaceAddresses = Collections.list(networkInterface.orElseThrow().getInetAddresses());
                var ipv4Address = interfaceAddresses.stream().filter(address -> address.getClass().equals(Inet4Address.class)).findFirst();
                if (ipv4Address.isPresent()) {
                    return Optional.of(ipv4Address.orElseThrow());
                }
            }
        } catch (SocketException ignored) {}

        return Optional.empty();
    }

    public static boolean mqttTopicMatchesSubscription(String topic, String subscription) {
        // Escape /
        String subscriptionRegex = subscription.replaceAll("/", "\\\\/");

        // Replace +
        subscriptionRegex = subscriptionRegex.replaceAll("\\+", "[^\\/]*");

        // Replace #
        subscriptionRegex = subscriptionRegex.replaceAll("\\\\/#$", "(\\/.*|\\$)");
        subscriptionRegex = subscriptionRegex.replaceAll("^#$", ".*");

        var pattern = Pattern.compile(subscriptionRegex);
        return pattern.matcher(topic).matches();
    }

    public static MqttPublishMessage mqttPublishMessageFromValues(String topic, byte[] payload) {
        return MqttMessageBuilders.publish()
            .topicName(topic)
            .payload(Unpooled.buffer().writeBytes(payload))
            .messageId(new Random().nextInt())
            .qos(MqttQoS.AT_LEAST_ONCE)
            .retained(false)
            .build();
    }

}
