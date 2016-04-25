package com.openddal.server.pgsql;

import java.io.InputStream;
import java.net.SocketAddress;

import com.openddal.server.ProtocolTransport;
import com.openddal.server.Request;
import com.openddal.server.Session;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

public class PgSQLRequest implements Request {


    private ProtocolTransport trans;

    public PgSQLRequest(ProtocolTransport trans) {
        this.trans = trans;
    }

    @Override
    public SocketAddress getRemoteAddress() {
        SocketAddress socketAddress = trans.getChannel().remoteAddress();
        return socketAddress;
    }

    @Override
    public SocketAddress getLocalAddress() {
        SocketAddress socketAddress = trans.getChannel().localAddress();
        return socketAddress;
    }

    @Override
    public Session getSession() {
        Channel channel = trans.getChannel();
        Session session = channel.attr(Session.CHANNEL_SESSION_KEY).get();
        return session;
    }

    @Override
    public ByteBuf getInputBuffer() {
        return trans.in;
    }

    @Override
    public InputStream getInputStream() {
        return trans.getInputStream();
    }
}
