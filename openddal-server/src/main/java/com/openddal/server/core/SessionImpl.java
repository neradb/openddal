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
package com.openddal.server.core;

import java.util.Map;
import java.util.Properties;

import com.openddal.engine.SessionInterface;
import com.openddal.server.NettyServer;
import com.openddal.server.mysql.parser.ServerParse;
import com.openddal.server.util.CharsetUtil;
import com.openddal.server.util.StringUtil;
import com.openddal.util.New;

import io.netty.channel.Channel;
import io.netty.util.AttributeKey;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 *
 */
public abstract class SessionImpl implements Session {

    private static final AttributeKey<SessionImpl> CHANNEL_SESSION_KEY = AttributeKey.valueOf("_CHANNEL_SESSION_KEY");


    private NettyServer server;
    private Channel channel;
    private Map<String, Object> attachments = New.hashMap();
    private String charset;
    private int charsetIndex;
    private String username;
    private String password;
    private String schema;
    private SessionInterface dbSession;

    public SessionImpl(NettyServer server) {
        this.server = server;
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

    public void bind(Channel channel) {
        this.channel = channel;
        Session old = channel.attr(CHANNEL_SESSION_KEY).get();
        if (old != null) {
            throw new IllegalStateException("session is already existing in channel");
        }
        channel.attr(CHANNEL_SESSION_KEY).set(this);

        Properties prop = new Properties();
        prop.setProperty("user", this.username);
        prop.setProperty("password", this.password);
        dbSession = server.getSessionFactory().createSession(prop);
    }

    public void close() {
        dbSession.close();
        attachments.clear();
        if (channel != null && channel.isOpen()) {
            channel.attr(CHANNEL_SESSION_KEY).remove();
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

    public static SessionImpl get(Channel chanel) {
        return chanel.attr(CHANNEL_SESSION_KEY).get();
    }

    /**
     * @return
     */
    public boolean isClosed() {
        // TODO Auto-generated method stub
        return false;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.openddal.server.core.Session#getThreadId()
     */
    @Override
    public long getThreadId() {
        // TODO Auto-generated method stub
        return 0;
    }


    public void query(String sql) throws Exception {
        int rs = ServerParse.parse(sql);
        switch (rs & 0xff) {
        case ServerParse.SET:
            processSet(sql, rs >>> 8);
            break;
        case ServerParse.SHOW:
            processShow(sql, rs >>> 8);
            break;
        case ServerParse.SELECT:
            processSelect(sql, rs >>> 8);
            break;
        case ServerParse.START:
            processStart(sql, rs >>> 8);
            break;
        case ServerParse.BEGIN:
            processBegin(sql, rs >>> 8);
            break;
        case ServerParse.LOAD:
            processSavepoint(sql, rs >>> 8);
            break;
        case ServerParse.SAVEPOINT:
            processSavepoint(sql, rs >>> 8);
            break;
        case ServerParse.USE:
            processUse(sql, rs >>> 8);
            break;
        case ServerParse.COMMIT:
            processCommit(sql, rs >>> 8);
            break;
        case ServerParse.ROLLBACK:
            processRollback(sql, rs >>> 8);
            break;
        case ServerParse.EXPLAIN:
            execute(sql, ServerParse.EXPLAIN);
            break;
        default:
            execute(sql, rs);
        }
    }

    public void initDB(String db) throws Exception {
    }

    private void processCommit(String sql, int offset) throws Exception {


    }

    private void processRollback(String sql, int offset) throws Exception {

    }

    private void processUse(String sql, int offset) throws Exception {
        String schema = sql.substring(offset).trim();
        int length = schema.length();
        if (length > 0) {
            if (schema.endsWith(";"))
                schema = schema.substring(0, schema.length() - 1);
            schema = StringUtil.replaceChars(schema, "`", null);
            length = schema.length();
            if (schema.charAt(0) == '\'' && schema.charAt(length - 1) == '\'') {
                schema = schema.substring(1, length - 1);
            }
        }
        initDB(schema);
    }

    private void processBegin(String sql, int offset) throws Exception {
    }

    private void processSavepoint(String sql, int offset) throws Exception {

    }

    private void processStart(String sql, int offset) throws Exception {

    }

    public void processSet(String stmt, int offset) throws Exception {

    }

    public void processShow(String stmt, int offset) throws Exception {

    }

    public void processSelect(String stmt, int offs) throws Exception {

    }

    public void processKill(String stmt, int offset) throws Exception {

    }

    private void execute(String sql, int type) throws Exception {

    }


}
