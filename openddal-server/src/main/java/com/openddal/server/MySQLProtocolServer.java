package com.openddal.server;

import com.openddal.server.mysql.processor.MySQLProcessorFactory;
import com.openddal.server.mysql.processor.MySQLRequestFactory;
import com.openddal.server.mysql.processor.MySQLResponseFactory;
import com.openddal.server.processor.ProcessorFactory;
import com.openddal.server.processor.RequestFactory;
import com.openddal.server.processor.ResponseFactory;
import io.netty.channel.ChannelHandler;

/**
 * 
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 *
 */
public class MySQLProtocolServer extends NettyServer {

    public MySQLProtocolServer(ServerArgs args) {
        super(args);
    }

    @Override
    protected ChannelHandler createProtocolDecoder() {
        return new ProtocolDecoder();
    }

    @Override
    protected ChannelHandler createProtocolEncoder() {
        return new ProtocolEncoder();
    }

    @Override
    protected ProcessorFactory createProcessorFactory() {
        return new MySQLProcessorFactory();
    }

    @Override
    protected RequestFactory createRequestFactory() {
        return new MySQLRequestFactory();
    }

    @Override
    protected ResponseFactory createResponseFactory() {
        return new MySQLResponseFactory();
    }
}
