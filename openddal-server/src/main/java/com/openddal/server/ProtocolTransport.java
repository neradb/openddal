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
package com.openddal.server;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.SocketAddress;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.Channel;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class ProtocolTransport {
    
    public static final int DEFAULT_BUFFER_SIZE = 16384;
    public final ByteBuf in;
    public final ByteBuf out;
    private final InputStream input;
    private final OutputStream output;
    private final Channel channel;

    public ProtocolTransport(Channel channel, ByteBuf in) {
        this.channel = channel;
        this.in = in;
        this.out = channel.alloc().buffer(DEFAULT_BUFFER_SIZE);
        this.input = new ByteBufInputStream(in);
        this.output = new ByteBufOutputStream(out);
    }
    
    public SocketAddress getRemoteAddress() {
        SocketAddress socketAddress = channel.remoteAddress();
        return socketAddress;
    }

    public SocketAddress getLocalAddress() {
        SocketAddress socketAddress = channel.localAddress();
        return socketAddress;
    }
    
    public Session getSession() {
        Session session = channel.attr(Session.CHANNEL_SESSION_KEY).get();
        return session;
    }
    
    public boolean isOpen() {
        return channel.isOpen();
    }

    public void close() {
        channel.close();
    }

    
    public Channel getChannel() {
        return channel;
    }

    /**
     * @return the input
     */
    public InputStream getInputStream() {
        return input;
    }

    /**
     * @return the output
     */
    public OutputStream getOutputStream() {
        return output;
    }

}
