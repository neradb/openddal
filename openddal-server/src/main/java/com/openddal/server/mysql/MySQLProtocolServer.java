package com.openddal.server.mysql;

import com.openddal.engine.Constants;
import com.openddal.server.Authenticator;
import com.openddal.server.NettyServer;
import com.openddal.server.RequestFactory;
import com.openddal.server.ResponseFactory;
import com.openddal.server.ServerArgs;
import com.openddal.server.processor.ProcessorFactory;

import io.netty.channel.ChannelHandler;

/**
 * 
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 *
 */
public class MySQLProtocolServer extends NettyServer {
    

    public static final String SERVER_NAME = "openddal-server-for-mysql";

    public static final String DEFAULT_CHARSET = "utf8";
    
    public static final byte PROTOCOL_VERSION = 10;

    public static final String SERVER_VERSION = "5.6.27" + SERVER_NAME + "-" + Constants.getFullVersion();

    public MySQLProtocolServer(ServerArgs args) {
        super(args);
    }

    @Override
    protected ChannelHandler createProtocolDecoder() {
        return new MySQLProtocolDecoder();
    }

    @Override
    protected ChannelHandler createProtocolEncoder() {
        return null;
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

    @Override
    protected Authenticator createAuthenticator() {
        return new MySQLAuthenticator();
    }

    @Override
    protected String getServerName() {
        return SERVER_NAME;
    }
}
