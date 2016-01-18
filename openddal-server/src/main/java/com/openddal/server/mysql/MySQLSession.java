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
import java.util.Map;

import com.openddal.server.mysql.proto.Handshake;
import com.openddal.server.mysql.proto.HandshakeResponse;
import com.openddal.server.processor.Session;
import com.openddal.util.JdbcUtils;
import com.openddal.util.New;

import io.netty.channel.Channel;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 *
 */
public class MySQLSession implements Session {

    private Channel channel;
    private final Handshake handshake;
    private final HandshakeResponse handshakeResponse;
    private Connection engineConnection;
    private Map<String, Object> attachments = New.hashMap();


    

    /**
     * @param channel
     * @param handshake
     * @param handshakeResponse
     */
    public MySQLSession(Handshake handshake, HandshakeResponse handshakeResponse) {
        super();
        this.handshake = handshake;
        this.handshakeResponse = handshakeResponse;
    }

    /**
     * @return the sessionId
     */
    public long getSessionId() {
        return handshake.connectionId;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T setAttachment(String key, T value) {
        T old = (T) attachments.get(key);
        attachments.put(key, value);
        return old;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T getAttachment(String key) {
        T val = (T) attachments.get(key);
        return val;
    }

    /**
     * @return the user
     */
    public String getUser() {
        return handshakeResponse.username;
    }


    /**
     * @return the charset
     */
    public String getCharset() {
        return Charsets.getCharset((int)handshake.characterSet);
    }

    /**
     * @return the schema
     */
    public String getSchema() {
        return handshakeResponse.schema;
    }


    /**
     * @return the engineConnection
     */
    @Override
    public Connection getEngineConnection() {
        return engineConnection;
    }

    /**
     * @param engineConnection the engineConnection to set
     */
    public void setEngineConnection(Connection engineConnection) {
        this.engineConnection = engineConnection;
    }
    

    public void bind(Channel channel) {
        this.channel = channel;
        Session old = channel.attr(Session.CHANNEL_SESSION_KEY).get();
        if (old != null) {
            throw new IllegalStateException("session is already existing in channel");
        }
        channel.attr(Session.CHANNEL_SESSION_KEY).set(this);
    }

    public void close() {
        JdbcUtils.closeSilently(getEngineConnection());
        attachments.clear();
        if(channel != null && channel.isOpen()) {
            channel.close();
        }
    }


}
