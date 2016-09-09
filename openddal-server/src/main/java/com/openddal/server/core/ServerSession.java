/*
 * Copyright 2014-2016 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
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
package com.openddal.server.core;

import java.util.Map;
import java.util.Properties;

import com.openddal.engine.Session;
import com.openddal.server.NettyServer;
import com.openddal.server.ServerException;
import com.openddal.server.util.CharsetUtil;
import com.openddal.util.New;

import io.netty.channel.Channel;
import io.netty.util.AttributeKey;

/**
 * 
 * @author jorgie.li
 *
 */
public class ServerSession implements AutoCloseable {

    private static final AttributeKey<ServerSession> CHANNEL_SESSION_KEY = AttributeKey.valueOf("_CHANNEL_SESSION_KEY");

    private NettyServer server;
    private Channel channel;
    private Map<String, Object> attachments = New.hashMap();
    private String charset;
    private int charsetIndex;
    private String username;
    private String password;
    private String schema;
    private final long threadId;
    private final long uptime;
    private Session dbSession;
    private QueryDispatcher dispatcher;


    public ServerSession(NettyServer server) {
        this.server = server;
        this.threadId = server.generateThreadId();
        this.dispatcher = server.newQueryDispatcher(this);
        this.uptime = System.currentTimeMillis();

    }

    @SuppressWarnings("unchecked")
    public <T> T setAttachment(String key, T value) {
        T old = (T) attachments.get(key);
        attachments.put(key, value);
        return old;
    }

    @SuppressWarnings("unchecked")
    public <T> T getAttachment(String key) {
        T val = (T) attachments.get(key);
        return val;
    }

    /**
     * @return the user
     */
    public String getUser() {
        return username;
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
        return schema;
    }

    public NettyServer getServer() {
        return server;
    }
    
    public Session getDbSession() {
        return dbSession;
    }

    public void bind(Channel channel) {
        this.channel = channel;
        ServerSession old = channel.attr(CHANNEL_SESSION_KEY).get();
        if (old != null) {
            throw new IllegalStateException("session is already existing in channel");
        }
        channel.attr(CHANNEL_SESSION_KEY).set(this);

        Properties prop = new Properties();
        if(this.username != null) {
            prop.setProperty("user", this.username);
        }
        if(this.password != null) {
            prop.setProperty("password", this.password);
        }
        dbSession = server.getEngine().createSession(prop);
        server.registerSession(this);
    }

    public void close() {
        dbSession.close();
        server.removeSession(threadId);
        if (channel != null && channel.isOpen()) {
            channel.attr(CHANNEL_SESSION_KEY).remove();
            channel.close();
        }
    }

    public boolean setCharsetIndex(int ci) {
        String charset = CharsetUtil.getDbCharset(ci);
        if (charset != null) {
            this.charset = CharsetUtil.getCharset(ci);
            this.charsetIndex = ci;
            return true;
        } else {
            return false;
        }
    }

    public boolean setCharset(String charset) {
        int ci = CharsetUtil.getDBIndex(charset);
        if (ci > 0) {
            this.charset = CharsetUtil.getCharset(ci);
            this.charsetIndex = ci;
            return true;
        } else {
            return false;
        }
    
    }

    public int getCharsetIndex() {
        return this.charsetIndex;
    }

    public void setUser(String username) {
        this.username = username;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public static ServerSession get(Channel chanel) {
        return chanel.attr(CHANNEL_SESSION_KEY).get();
    }

    /**
     * @return
     */
    public boolean isClosed() {
        return dbSession.isClosed();
    }


    public long getUptime() {
        return uptime;
    }

    public long getThreadId() {
        return threadId;
    }
    
    public QueryResult executeQuery(String query) throws ServerException {
        QueryProcessor processor = dispatcher.dispatch(query);
        QueryResult result = processor.process(query);
        return result;
    }
}
