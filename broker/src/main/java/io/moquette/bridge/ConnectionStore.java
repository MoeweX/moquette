package io.moquette.bridge;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

public class ConnectionStore {
    private Set<InetSocketAddress> intendedConnections;
    private CopyOnWriteArrayList<BridgeConnection> actualConnections;

    public ConnectionStore() {
        this.intendedConnections = ConcurrentHashMap.newKeySet();
        this.actualConnections = new CopyOnWriteArrayList<>();
    }

    public ConnectionStore(List<InetSocketAddress> intendedConnections) {
        this();
        this.intendedConnections.addAll(intendedConnections);
    }

    public List<BridgeConnection> getAllDistinct() {
        return actualConnections.stream()
            .distinct()
            .collect(Collectors.toList());
    }

    public List<String> getAllDistinctRemoteIds() {
        return getAllDistinct().stream()
            .map(BridgeConnection::getRemoteId)
            .collect(Collectors.toList());
    }

    public void addActualConnection(BridgeConnection connection) {
        actualConnections.add(connection);
    }

    public void addIntendedConnection(InetSocketAddress address) {
        intendedConnections.add(address);
    }

    public void removeIntendedConnection(InetSocketAddress address) {
        intendedConnections.remove(address);
    }

    public List<InetSocketAddress> missingConnections() {
        List<InetSocketAddress> missingConnections = new ArrayList<>(intendedConnections);
        List<InetSocketAddress> pendingOrConnected = actualConnections.stream()
            .filter(BridgeConnection::isClient)
            .map(BridgeConnection::getRemoteAddress)
            .collect(Collectors.toList());
        missingConnections.removeAll(pendingOrConnected);
        return missingConnections;
    }
}
