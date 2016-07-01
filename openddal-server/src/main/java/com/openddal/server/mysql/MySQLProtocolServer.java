package com.openddal.server.mysql;

import com.openddal.engine.Constants;
import com.openddal.server.NettyServer;
import com.openddal.server.ProtocolHandler;
import com.openddal.server.ServerArgs;

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
    protected String getServerName() {
        return SERVER_NAME;
    }

    @Override
    protected ChannelHandler createProtocolDecoder() {
        return new MySQLProtocolDecoder();
    }
    
    @Override
    protected ProtocolHandler createProtocolHandler() {
        return new MySQLProtocolHandler();
    }
    
    @Override
    protected ProtocolHandler createHandshakeHandler() {
        return new MySQLHandshakeHandler();
    }
}
