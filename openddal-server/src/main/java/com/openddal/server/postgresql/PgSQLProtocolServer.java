package com.openddal.server.postgresql;

import com.openddal.server.Authenticator;
import com.openddal.server.NettyServer;
import com.openddal.server.RequestFactory;
import com.openddal.server.ResponseFactory;
import com.openddal.server.ServerArgs;
import com.openddal.server.processor.ProcessorFactory;

import io.netty.channel.ChannelHandler;

public class PgSQLProtocolServer extends NettyServer {

    public PgSQLProtocolServer(ServerArgs args) {
        super(args);
    }

    @Override
    protected ChannelHandler createProtocolDecoder() {
        return null;
    }

    @Override
    protected ChannelHandler createProtocolEncoder() {
        return null;
    }

    @Override
    protected ProcessorFactory createProcessorFactory() {
        return null;
    }

    @Override
    protected RequestFactory createRequestFactory() {
        return null;
    }

    @Override
    protected ResponseFactory createResponseFactory() {
        return null;
    }

    @Override
    protected Authenticator createAuthenticator() {
        return null;
    }

   
    @Override
    protected String getServerName() {
        return null;
    }
}
