package io.moquette.delaygrouping.connections;

import java.net.InetSocketAddress;

public interface Connection {
    public String getRemoteId();
    public InetSocketAddress getRemoteAddress();
    public InetSocketAddress getIntendedRemoteAddress();
    public void setIntendedRemoteAddress(InetSocketAddress address);
    public Boolean isConnected();
    public Boolean isClient();
}
