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

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import com.openddal.server.MySQLProtocolServer;
import com.openddal.server.mysql.proto.ERR;
import com.openddal.server.mysql.proto.Flags;
import com.openddal.server.mysql.proto.Handshake;
import com.openddal.server.mysql.proto.HandshakeResponse;
import com.openddal.server.mysql.proto.OK;
import com.openddal.server.processor.Authenticator;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 *
 */
public class MySQLAuthenticator implements Authenticator {
    private static final AtomicLong connIdGenerator = new AtomicLong(0);
    @Override
    public void onConnected(Channel channel) {
        ByteBuf out = channel.alloc().buffer();
        try {
            Handshake handshake = new Handshake();
            handshake.protocolVersion = 0x0a;
            handshake.serverVersion = "";
            handshake.connectionId = connIdGenerator.incrementAndGet();
            handshake.challenge1 = getRandomString(8);
            handshake.capabilityFlags = Flags.CLIENT_BASIC_FLAGS;
            handshake.characterSet = Charsets.getIndex(MySQLProtocolServer.DEFAULT_CHARSET);
            handshake.statusFlags = Flags.SERVER_STATUS_AUTOCOMMIT;
            handshake.challenge2 = getRandomString(12);
            handshake.authPluginDataLength = 21;
            handshake.authPluginName = "mysql_native_password";
            // Remove some flags from the reply
            handshake.removeCapabilityFlag(Flags.CLIENT_COMPRESS);
            handshake.removeCapabilityFlag(Flags.CLIENT_IGNORE_SPACE);
            handshake.removeCapabilityFlag(Flags.CLIENT_PROTOCOL_41);
            handshake.removeCapabilityFlag(Flags.CLIENT_LOCAL_FILES);
            handshake.removeCapabilityFlag(Flags.CLIENT_SSL);
            handshake.removeCapabilityFlag(Flags.CLIENT_TRANSACTIONS);
            handshake.removeCapabilityFlag(Flags.CLIENT_RESERVED);
            handshake.removeCapabilityFlag(Flags.CLIENT_PROTOCOL_41);
            // Set the default result set creation to the server's character set            
            channel.writeAndFlush(out);
        } finally {
            out.release();
        }
        
    }

    @Override
    public void authorize(Channel channel, ByteBuf buf) {
        HandshakeResponse authReply = null;
        try {
            byte[] packet = new byte[buf.readableBytes()];
            buf.readBytes(packet);
            authReply = HandshakeResponse.loadFromPacket(packet);
            if (!authReply.hasCapabilityFlag(Flags.CLIENT_PROTOCOL_41)) {
                writeErrMessage(channel, ErrorCode.ER_DBACCESS_DENIED_ERROR,"We do not support Protocols under 4.1");
                return;
            }
            OK ok = new OK();
            ByteBuf out = channel.alloc().buffer();
            buf.writeBytes(ok.toPacket());
            channel.writeAndFlush(out);            
        } catch (Exception e) {
            writeErrMessage(channel, ErrorCode.ER_DBACCESS_DENIED_ERROR, "Access denied for user '" + authReply.username + "' to database '"
                    + authReply.schema + "'");
        } finally {
            buf.release();
        }    
    }
    
    public static String getRandomString(int length) {   
        char[] chars = new char[length];
        Random random = new Random();   
        for (int i = 0; i < length; i ++) {   
            chars[i] = (char)random.nextInt(127);
        }   
        return String.valueOf(chars);
    }  

    
    private static void writeErrMessage(Channel channel, int errno,
            String msg) {
        ERR err = new ERR();
        err.errorCode = errno;
        err.errorMessage = msg;
        ByteBuf buf = channel.alloc().buffer();
        buf.writeBytes(err.toPacket());
        channel.writeAndFlush(buf);
    }

}
