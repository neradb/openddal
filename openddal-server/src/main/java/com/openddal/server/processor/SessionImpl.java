package com.openddal.server.processor;

import java.sql.Connection;
import java.util.Map;

import com.openddal.util.New;

import io.netty.util.AttributeKey;

public class SessionImpl implements Session {

    public static final AttributeKey<Session> SESSION_KEY = AttributeKey.valueOf("_PROTOCOL_SESSION_KEY");

    private final long sessionId;
    private State state = State.NEW;
    private String user;
    private String charset;
    private String schema;
    private Connection engineConnection;
    private Map<String, Object> attachments = New.hashMap();

    public SessionImpl(long sessionId) {
        this.sessionId = sessionId;
    }
    

    /**
     * @return the sessionId
     */
    public long getSessionId() {
        return sessionId;
    }


    @SuppressWarnings("unchecked")
    @Override
    public <T> T setAttachment(String key, T value) {
        T old = (T)attachments.get(key);
        attachments.put(key, value);
        return old;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T getAttachment(String key) {
        T val = (T)attachments.get(key);
        return val;
    }

    /**
     * @return the state
     */
    public State getState() {
        return state;
    }

    /**
     * @param state the state to set
     */
    public void setState(State state) {
        this.state = state;
    }

    /**
     * @return the user
     */
    public String getUser() {
        return user;
    }

    /**
     * @param user the user to set
     */
    public void setUser(String user) {
        this.user = user;
    }

    /**
     * @return the charset
     */
    public String getCharset() {
        return charset;
    }

    /**
     * @param charset the charset to set
     */
    public void setCharset(String charset) {
        this.charset = charset;
    }

    /**
     * @return the schema
     */
    public String getSchema() {
        return schema;
    }

    /**
     * @param schema the schema to set
     */
    public void setSchema(String schema) {
        this.schema = schema;
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
    
}
