package io.moquette.delaygrouping.bridge;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

class ConnectionStore {
    private List<InetSocketAddress> configuredConnections;
    private CopyOnWriteArrayList<BridgeConnection> connections;

    ConnectionStore(List<InetSocketAddress> configuredConnections) {
        this.configuredConnections = configuredConnections;
        this.connections = new CopyOnWriteArrayList<>();
    }

    List<BridgeConnection> getAllDistinct() {
        return connections.stream()
            .distinct()
            .collect(Collectors.toList());
    }

    List<String> getAllDistinctRemoteBridgeIds() {
        return connections.stream()
            .distinct()
            .map(BridgeConnection::getRemoteBridgeId)
            .collect(Collectors.toList());
    }

    void add(BridgeConnection connection) {
        connections.add(connection);
    }

    List<InetSocketAddress> missingConnections() {
        List<InetSocketAddress> missingConnections = this.configuredConnections;
        List<InetSocketAddress> pendingOrConnected = connections.stream()
            .filter(BridgeConnection::isClient)
            .map(BridgeConnection::getConfiguredRemoteAddr)
            .collect(Collectors.toList());
        missingConnections.removeAll(pendingOrConnected);
        return missingConnections;
    }
}
