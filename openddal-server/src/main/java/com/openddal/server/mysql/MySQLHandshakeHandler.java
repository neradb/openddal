/*
 * Copyright 2014-2016 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the “License”);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an “AS IS” BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.openddal.server.mysql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.openddal.server.NettyServer;
import com.openddal.server.core.Session;
import com.openddal.server.core.SessionImpl;
import com.openddal.server.mysql.auth.Privilege;
import com.openddal.server.mysql.proto.ERR;
import com.openddal.server.mysql.proto.Flags;
import com.openddal.server.mysql.proto.Handshake;
import com.openddal.server.mysql.proto.HandshakeResponse;
import com.openddal.server.mysql.proto.OK;
import com.openddal.server.util.CharsetUtil;
import com.openddal.server.util.ErrorCode;
import com.openddal.server.util.StringUtil;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 *
 */
public class MySQLHandshakeHandler extends ChannelInboundHandlerAdapter {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLHandshakeHandler.class);
    private final long threadId;
    private NettyServer server;
    private SessionImpl session;

    public MySQLHandshakeHandler(NettyServer server) {
        this.server = server;
        this.threadId = server.generatethreadId();
        this.session = new SessionImpl(server);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        ByteBuf out = ctx.alloc().buffer();
        Handshake handshake = new Handshake();
        handshake.sequenceId = 0;
        handshake.protocolVersion = MySQLServer.PROTOCOL_VERSION;
        handshake.serverVersion = MySQLServer.SERVER_VERSION;
        handshake.connectionId = threadId;
        handshake.challenge1 = StringUtil.getRandomString(8);
        handshake.characterSet = CharsetUtil.getIndex(MySQLServer.DEFAULT_CHARSET);
        handshake.statusFlags = Flags.SERVER_STATUS_AUTOCOMMIT;
        handshake.challenge2 = StringUtil.getRandomString(12);
        handshake.authPluginDataLength = 21;
        handshake.authPluginName = Flags.MYSQL_NATIVE_PASSWORD;
        handshake.capabilityFlags = Flags.CLIENT_BASIC_FLAGS;
        handshake.removeCapabilityFlag(Flags.CLIENT_COMPRESS);
        handshake.removeCapabilityFlag(Flags.CLIENT_SSL);
        handshake.removeCapabilityFlag(Flags.CLIENT_LOCAL_FILES);
        session.setCharsetIndex((int) handshake.characterSet);
        session.setAttachment("seed", handshake.challenge1 + handshake.challenge2);
        out.writeBytes(handshake.toPacket());
        ctx.writeAndFlush(out);
    
    }



    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (SessionImpl.get(ctx.channel()) == null) {
            authenticate(ctx, msg);        
        } else {
            ctx.fireChannelRead(msg);
        }
    
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        Session session = SessionImpl.get(ctx.channel());
        if (session != null) {
            session.close();
        }
    }
    
    

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOGGER.warn("exceptionCaught", cause);
        ctx.close();
    }


    private void authenticate(ChannelHandlerContext ctx, Object msg) {
        Privilege privilege = server.getPrivilege();
        HandshakeResponse authReply = null;
        ByteBuf buf = (ByteBuf) msg;
        try {
            byte[] packet = new byte[buf.readableBytes()];
            buf.readBytes(packet);
            authReply = HandshakeResponse.loadFromPacket(packet);
            
            if (!authReply.hasCapabilityFlag(Flags.CLIENT_PROTOCOL_41)) {
                error(ctx, ErrorCode.ER_NOT_SUPPORTED_AUTH_MODE, "We do not support Protocols under 4.1");
                return;
            }
            
            if (!privilege.userExists(authReply.username)) {
                error(ctx, ErrorCode.ER_ACCESS_DENIED_ERROR,
                        "Access denied for user '" + authReply.username + "'");
                return;
            }
            
            if (!StringUtil.isEmpty(authReply.schema) 
                    && !privilege.schemaExists(authReply.username, authReply.schema)) {
                String s = "Access denied for user '" + authReply.username
                        + "' to database '" + authReply.schema + "'";
                error(ctx, ErrorCode.ER_DBACCESS_DENIED_ERROR, s);
                return;
            }

            String seed = session.getAttachment("seed");
            if (!privilege.checkPassword(authReply.username, authReply.authResponse, seed)) {
                error(ctx, ErrorCode.ER_ACCESS_DENIED_ERROR,
                        "Access denied for user '" + authReply.username + "'");
                return;
            }
            session.setUser(authReply.username);
            session.setSchema(authReply.schema);
            session.bind(ctx.channel());
            session.setAttachment("remoteAddress", ctx.channel().remoteAddress().toString());
            session.setAttachment("localAddress", ctx.channel().localAddress().toString());
            success(ctx);
        } catch (Exception e) {
            String errMsg = authReply == null ? e.getMessage()
                    : "Access denied for user '" + authReply.username + "' to database '" + authReply.schema + "'";
            LOGGER.error("Authorize failed. " + errMsg, e);
            error(ctx, ErrorCode.ER_DBACCESS_DENIED_ERROR, errMsg);
        } finally {
            buf.release();
        }
    }
    
        

    private void success(ChannelHandlerContext ctx) {
        ByteBuf out = ctx.alloc().buffer();
        OK ok = new OK();
        ok.sequenceId = 2;
        ok.setStatusFlag(Flags.SERVER_STATUS_AUTOCOMMIT);
        out.writeBytes(ok.toPacket());
        ctx.writeAndFlush(out);
    }
    
    public void error(ChannelHandlerContext ctx, int errno, String msg) {
        ByteBuf out = ctx.alloc().buffer();
        ERR err = new ERR();
        err.sequenceId = 2;
        err.errorCode = errno;
        err.errorMessage = msg;
        out.writeBytes(err.toPacket());
        LOGGER.info(msg);
    }




}
