package io.moquette.delaygrouping.monitoring;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class MonitoringHandler extends ChannelInboundHandlerAdapter {

    private MonitoringConnection connection;

    public MonitoringHandler(MonitoringConnection connection) {
        this.connection = connection;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        connection.handleChannelRead((String) msg);
    }

    public MonitoringConnection getConnection() {
        return connection;
    }
}
