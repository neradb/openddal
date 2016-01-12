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
package com.openddal.server.processor;

import com.openddal.server.ProtocolTransport;
import io.netty.buffer.ByteBuf;
import io.netty.util.AttributeKey;

import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 *
 */
public class RequestFactory {
    private static RequestFactory instance = new RequestFactory();

    private static AtomicLong sessionIdGenerator = new AtomicLong(0);

    private RequestFactory() {
    }

    public static RequestFactory getInstance() {
        return instance;
    }

    public Request createRequest(ProtocolTransport trans) {
        return new RequestImp(trans);
    }

    private static class RequestImp implements Request {

        private static final AttributeKey<Session> SESSION_KEY =
                AttributeKey.valueOf("_PROTOCOL_SESSION_KEY");
        
        private ProtocolTransport trans;

        private RequestImp(ProtocolTransport trans) {
            this.trans = trans;
        }

        @Override
        public SocketAddress getRemoteAddress() {
            SocketAddress socketAddress = trans.getChannel().remoteAddress();
            return socketAddress;
        }

        @Override
        public SocketAddress getLocalAddress() {
            SocketAddress socketAddress = trans.getChannel().localAddress();
            return socketAddress;
        }

        @Override
        public Session getSession() {
            Session session = trans.getChannel().attr(SESSION_KEY).get();
            if (session == null) {
                session = new SessionImp(trans);
                session.setState(Session.State.CONNECTIONING);
            }
            trans.getChannel().attr(SESSION_KEY).set(session);
            return session;
        }

        @Override
        public ByteBuf getInputByteBuf() {
            return trans.in;
        }
    }

    private static class SessionImp implements Session {

        public final long sessionID;
        private final ProtocolTransport trans;


        private SessionImp(ProtocolTransport trans) {
            this.trans = trans;
            this.sessionID = sessionIdGenerator.incrementAndGet();
        }

        @Override
        public <T> T setAttachment(String key, T value) {
            return null;
        }

        @Override
        public <T> T getAttachment(String key) {
            return null;
        }

        @Override
        public long getSessionID() {
            return sessionID;
        }

        @Override
        public String getUser() {
            return null;
        }

        @Override
        public void setUser(String user) {

        }

        @Override
        public State getState() {
            return null;
        }

        @Override
        public void setState(State state) {

        }
    }
}
