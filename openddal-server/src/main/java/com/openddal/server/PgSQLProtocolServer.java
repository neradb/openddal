package com.openddal.server;

import com.openddal.server.processor.ProcessorFactory;
import com.openddal.server.processor.RequestFactory;
import com.openddal.server.processor.ResponseFactory;
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
}
