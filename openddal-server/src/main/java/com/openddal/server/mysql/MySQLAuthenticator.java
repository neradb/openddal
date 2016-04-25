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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.openddal.engine.Constants;
import com.openddal.jdbc.JdbcDriver;
import com.openddal.server.Authenticator;
import com.openddal.server.mysql.proto.ERR;
import com.openddal.server.mysql.proto.Flags;
import com.openddal.server.mysql.proto.Handshake;
import com.openddal.server.mysql.proto.HandshakeResponse;
import com.openddal.server.mysql.proto.OK;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.AttributeKey;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 *
 */
public class MySQLAuthenticator implements Authenticator {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLAuthenticator.class);
    
    private final AtomicLong connIdGenerator = new AtomicLong(0);
    private final AttributeKey<MySQLSession> TMP_SESSION_KEY = AttributeKey.valueOf("_AUTHTMP_SESSION_KEY");

    @Override
    public void onConnected(Channel channel) {
        ByteBuf out = channel.alloc().buffer();
        Handshake handshake = new Handshake();
        handshake.protocolVersion = MySQLProtocolServer.PROTOCOL_VERSION;
        handshake.serverVersion = MySQLProtocolServer.SERVER_VERSION;
        handshake.connectionId = connIdGenerator.incrementAndGet();
        handshake.challenge1 = getRandomString(8);
        handshake.capabilityFlags = Flags.CLIENT_BASIC_FLAGS;
        handshake.characterSet = MySQLCharsets.getIndex(MySQLProtocolServer.DEFAULT_CHARSET);
        handshake.statusFlags = Flags.SERVER_STATUS_AUTOCOMMIT;
        handshake.challenge2 = getRandomString(12);
        handshake.authPluginDataLength = 21;
        handshake.authPluginName = "mysql_native_password";
        // Remove some flags from the reply
        handshake.removeCapabilityFlag(Flags.CLIENT_COMPRESS);
        handshake.removeCapabilityFlag(Flags.CLIENT_IGNORE_SPACE);
        handshake.removeCapabilityFlag(Flags.CLIENT_LOCAL_FILES);
        handshake.removeCapabilityFlag(Flags.CLIENT_SSL);
        handshake.removeCapabilityFlag(Flags.CLIENT_TRANSACTIONS);
        handshake.removeCapabilityFlag(Flags.CLIENT_RESERVED);
        handshake.removeCapabilityFlag(Flags.CLIENT_REMEMBER_OPTIONS);

        // handshake = Handshake.loadFromPacket(packet);
        MySQLSession temp = new MySQLSession();
        temp.setHandshake(handshake);
        channel.attr(TMP_SESSION_KEY).set(temp);
        out.writeBytes(handshake.toPacket());
        channel.writeAndFlush(out);
    }

    @Override
    public void authorize(Channel channel, ByteBuf buf) {
        MySQLSession session = channel.attr(TMP_SESSION_KEY).getAndRemove();
        HandshakeResponse authReply = null;
        try {
            byte[] packet = new byte[buf.readableBytes()];
            buf.readBytes(packet);
            authReply = HandshakeResponse.loadFromPacket(packet);
            Connection connect = connectEngine(authReply);
            session.setHandshakeResponse(authReply);
            session.setEngineConnection(connect);
            session.bind(channel);
            success(channel);
        } catch (Exception e) {
            String msg = authReply == null ? e.getMessage()
                    : "Access denied for user '" + authReply.username + "' to database '" + authReply.schema + "'";
            LOGGER.error("Authorize failed. " + msg, e);
            error(channel, MySQLErrorCode.ER_DBACCESS_DENIED_ERROR, msg);
        } finally {
            buf.release();
        }
    }

    /**
     * @param authReply
     * @return
     * @throws SQLException
     */
    private Connection connectEngine(HandshakeResponse authReply) throws SQLException {
        Properties prop = new Properties();
        prop.setProperty("user", authReply.username);
        prop.setProperty("password", authReply.authResponse);
        Connection connect = JdbcDriver.load().connect(Constants.START_URL, prop);
        return connect;
    }

    public static String getRandomString(int length) {
        char[] chars = new char[length];
        Random random = new Random();
        for (int i = 0; i < length; i++) {
            chars[i] = (char) random.nextInt(127);
        }
        return String.valueOf(chars);
    }

    /**
     * @param channel
     * @param buf
     * @return
     */
    private void success(Channel channel) {
        ByteBuf out = channel.alloc().buffer();
        OK ok = new OK();
        ok.sequenceId = 2;
        ok.setStatusFlag(Flags.SERVER_STATUS_AUTOCOMMIT);
        out.writeBytes(ok.toPacket());
        channel.writeAndFlush(out);
    }
    

    private static void error(Channel channel, int errno, String msg) {
        ByteBuf out = channel.alloc().buffer();
        ERR err = new ERR();
        err.errorCode = errno;
        err.errorMessage = msg;
        out.writeBytes(err.toPacket());
        channel.writeAndFlush(out);
    }

}
