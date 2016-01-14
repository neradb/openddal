package com.openddal.server.mysql.processor;

import java.net.SocketAddress;

import com.openddal.server.ProtocolTransport;
import com.openddal.server.processor.Request;
import com.openddal.server.processor.Session;
import com.openddal.server.processor.SessionImpl;

import io.netty.buffer.ByteBuf;

public class MySQLRequest implements Request {


    private ProtocolTransport trans;

    public MySQLRequest(ProtocolTransport trans) {
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
        Session session = trans.attr(SessionImpl.SESSION_KEY).get();
        if (session == null) {
            session = new SessionImpl(trans);
            session.setState(Session.State.CONNECTIONING);
        }
        trans.attr(SessionImpl.SESSION_KEY).set(session);
        return session;
    }

    @Override
    public ByteBuf getInputBuffer() {
        return trans.in;
    }
}
