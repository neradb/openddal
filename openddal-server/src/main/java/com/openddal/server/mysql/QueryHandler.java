package com.openddal.server.mysql;

import org.slf4j.Logger;

import io.netty.channel.ChannelHandlerContext;

/**
 * @author roger
 */
public class QueryHandler {

    static Logger _log = UberUtil.getThisLogger();
    private MysqlServerHandler serverHandler;
    
    public QueryHandler(MysqlServerHandler severHandler) {
        this.serverHandler = severHandler;
    }

    public void query(ChannelHandlerContext ctx, QueryPacket packet) throws Exception {
        CharStream sql= packet.getSql();
        Object result = null;
        if (sql == null) {
            serverHandler.writeErrMessage(ctx, MysqlErrorCode.ER_ERROR_WHEN_EXECUTING_COMMAND, "Empty query.");
        } 
        else {
            result = serverHandler.session.run(sql);
        	Helper.writeResonpse(ctx, serverHandler, result, true);
        }
    }
}