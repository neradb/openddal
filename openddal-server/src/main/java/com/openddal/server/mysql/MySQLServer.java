package com.openddal.server.mysql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.openddal.engine.Constants;
import com.openddal.server.NettyServer;
import com.openddal.server.ServerArgs;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;

/**
 * 
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 *
 */
public class MySQLServer extends NettyServer {

    private static final Logger logger = LoggerFactory.getLogger(MySQLServer.class);

    public static final String DEFAULT_CHARSET = "utf8";
    public static final byte PROTOCOL_VERSION = 10;
    public static final String VERSION_COMMENT = "OpenDDAL MySQL Protocol Server";
    public static final String SERVER_VERSION = "5.6.31" + VERSION_COMMENT + "-" + Constants.getFullVersion();

    private final MySQLServerDecoder decoder = new MySQLServerDecoder();

    public MySQLServer(ServerArgs args) {
        super(args);
    }

    @Override
    protected String getServerName() {
        return VERSION_COMMENT;
    }

    @Override
    protected ChannelHandler newChannelInitializer() {
        return new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) throws Exception {
                if (logger.isDebugEnabled()) {
                    logger.debug("channel initialized with remote address {}", ch.remoteAddress());
                }
                MySQLServerHandler handler = new MySQLServerHandler(MySQLServer.this);
                ch.pipeline().addLast(decoder, handler);
            }
        };
    }

}
