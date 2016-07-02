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

import com.openddal.server.Session;
import com.openddal.server.mysql.proto.Handshake;
import com.openddal.server.mysql.proto.HandshakeResponse;
import com.openddal.server.util.CharsetUtil;
import com.openddal.util.JdbcUtils;
import com.openddal.util.New;

import io.netty.channel.Channel;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 *
 */
public class MySQLSession implements Session {

    private Channel channel;
    private Handshake handshake;
    private HandshakeResponse handshakeResponse;
    private Connection engineConnection;
    private Map<String, Object> attachments = New.hashMap();
    private String charset;
    private int charsetIndex;

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
        return this.charset;
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

    /**
     * @param handshake the handshake to set
     */
    public void setHandshake(Handshake handshake) {
        this.handshake = handshake;
    }

    /**
     * @param handshakeResponse the handshakeResponse to set
     */
    public void setHandshakeResponse(HandshakeResponse handshakeResponse) {
        this.handshakeResponse = handshakeResponse;

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
        if (channel != null && channel.isOpen()) {
            channel.attr(Session.CHANNEL_SESSION_KEY).remove();
            channel.close();
        }
    }

    public boolean setCharsetIndex(int ci) {
        String charset = CharsetUtil.getCharset(ci);
        if (charset != null) {
            return setCharset(charset);
        } else {
            return false;
        }
    }

    public boolean setCharset(String charset) {
        if (charset != null) {
            charset = charset.replace("'", "");
        }
        int ci = CharsetUtil.getIndex(charset);
        if (ci > 0) {
            this.charset = charset.equalsIgnoreCase("utf8mb4") ? "utf8" : charset;
            this.charsetIndex = ci;
            return true;
        } else {
            return false;
        }
    }

    @Override
    public int getCharsetIndex() {
        return this.charsetIndex;
    }
}
