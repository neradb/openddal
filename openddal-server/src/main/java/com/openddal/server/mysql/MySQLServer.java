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
public class MySQLServer extends NettyServer {
    


    public static final String DEFAULT_CHARSET = "utf8";
    
    public static final byte PROTOCOL_VERSION = 10;

    public static final String VERSION_COMMENT = "OpenDDAL MySQL Protocol Server";
    public static final String SERVER_VERSION = "5.6.31" + VERSION_COMMENT + "-" + Constants.getFullVersion();

    public MySQLServer(ServerArgs args) {
        super(args);
    }


    @Override
    protected String getServerName() {
        return VERSION_COMMENT;
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
        return new AuthHandler();
    }
}
