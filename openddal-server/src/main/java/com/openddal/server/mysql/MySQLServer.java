package com.openddal.server.mysql;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.openddal.engine.Constants;
import com.openddal.server.NettyServer;
import com.openddal.server.ServerArgs;
import com.openddal.server.core.QueryDispatcher;
import com.openddal.server.core.ServerSession;
import com.openddal.server.mysql.pcs.QueryDispatcherImpl;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;

/**
 * 
 * @author jorgie.li
 *
 */
public class MySQLServer extends NettyServer {

    private static final Logger logger = LoggerFactory.getLogger(MySQLServer.class);
    public static final String DEFAULT_CHARSET = "utf8";
    public static final byte PROTOCOL_VERSION = 10;
    public static final String VERSION_COMMENT = "OpenDDAL MySQL Protocol Server";
    public static final String SERVER_VERSION = "5.6.31" + VERSION_COMMENT + "-" + Constants.getFullVersion();

    public MySQLServer(ServerArgs args) {
        super(args);
    }

    @Override
    public void init() {
        super.init();
        Map<String, String> variables = getVariables();
        
        variables.put("auto_increment_increment", "1");
        variables.put("auto_increment_offset", "1");
        variables.put("autocommit", "1");
        
        variables.put("character_set_client", "utf8");
        variables.put("character_set_connection", "utf8");
        variables.put("character_set_results", "utf8");
        variables.put("character_set_server", "utf8");
        variables.put("character_set_database", "utf8");
        variables.put("init_connect", "");
        variables.put("interactive_timeout", "172800");
        variables.put("lower_case_table_names", "1");
        variables.put("max_allowed_packet", "16777216");
        variables.put("net_buffer_length", "8192");
        variables.put("net_write_timeout", "60");
        variables.put("query_cache_size", "0");
        variables.put("query_cache_type", "OFF");
        variables.put("sql_mode", "STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION");
        variables.put("system_time_zone", "CST");
        variables.put("time_zone", "SYSTEM");
        variables.put("lower_case_table_names", "1");
        variables.put("tx_isolation", "REPEATABLE-READ");
        variables.put("wait_timeout", "172800");
    }
    

    @Override
    protected String getServerName() {
        return VERSION_COMMENT;
    }
    
    @Override
    public QueryDispatcher newQueryDispatcher(ServerSession session) {
        return new QueryDispatcherImpl(session);
    }

    @Override
    protected ChannelHandler newChannelInitializer() {
        return new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) throws Exception {
                if (logger.isDebugEnabled()) {
                    logger.debug("channel initialized with remote address {}", ch.remoteAddress());
                }
                MySQLServerDecoder decoder = new MySQLServerDecoder();
                MySQLServerHandler handler = new MySQLServerHandler(MySQLServer.this);
                ch.pipeline().addLast(decoder, handler);
            }
        };
    }

}
