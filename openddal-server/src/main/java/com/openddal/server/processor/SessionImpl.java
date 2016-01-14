package com.openddal.server.processor;

import java.sql.Connection;

import com.openddal.server.ProtocolTransport;

import io.netty.util.AttributeKey;

public class SessionImpl implements Session {

    public static final AttributeKey<Session> SESSION_KEY = AttributeKey.valueOf("_PROTOCOL_SESSION_KEY");

    private long sessionId;
    private State state;
    private String user;
    private String charset;
    private String schema;
    private Connection engineConnection;
    private ProtocolTransport trans;

    public SessionImpl(ProtocolTransport trans) {
        super();
        this.trans = trans;
    }
    

    /**
     * @return the sessionId
     */
    public long getSessionId() {
        return sessionId;
    }


    /**
     * @param sessionId the sessionId to set
     */
    public void setSessionId(long sessionId) {
        this.sessionId = sessionId;
    }


    @Override
    public <T> void setAttachment(String key, T value) {
        AttributeKey<T> attrKey = AttributeKey.valueOf(key);
        trans.getChannel().attr(attrKey).set(value);
    }

    @Override
    public <T> T getAttachment(String key) {
        AttributeKey<T> attrKey = AttributeKey.valueOf(key);
        return trans.getChannel().attr(attrKey).get();
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
