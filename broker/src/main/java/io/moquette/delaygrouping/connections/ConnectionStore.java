package io.moquette.delaygrouping.connections;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

public class ConnectionStore<T extends Connection> {
    private List<InetSocketAddress> intendedConnections;
    private CopyOnWriteArrayList<T> actualConnections;

    public ConnectionStore(List<InetSocketAddress> intendedConnections) {
        this.intendedConnections = intendedConnections;
        this.actualConnections = new CopyOnWriteArrayList<>();
    }

    public List<T> getAllDistinct() {
        return actualConnections.stream()
            .distinct()
            .collect(Collectors.toList());
    }

    public List<String> getAllDistinctRemoteIds() {
        return getAllDistinct().stream()
            .map(Connection::getRemoteId)
            .collect(Collectors.toList());
    }

    public void add(T connection) {
        actualConnections.add(connection);
    }

    public List<InetSocketAddress> missingConnections() {
        List<InetSocketAddress> missingConnections = this.intendedConnections;
        List<InetSocketAddress> pendingOrConnected = actualConnections.stream()
            .filter(Connection::isClient)
            .map(Connection::getIntendedRemoteAddress)
            .collect(Collectors.toList());
        missingConnections.removeAll(pendingOrConnected);
        return missingConnections;
    }
}
