package io.moquette.delaygrouping;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Collections;
import java.util.Optional;

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
}
