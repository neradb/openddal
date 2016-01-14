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

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class ProtocolTransport {
    
    public static final int DEFAULT_BUFFER_SIZE = 1024;
    public ByteBuf in;
    public ByteBuf out;
    private Channel channel;

    public ProtocolTransport(Channel channel, ByteBuf in) {
        this.channel = channel;
        this.in = in;
        out = channel.alloc().buffer(DEFAULT_BUFFER_SIZE);
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
     * @param key
     * @return
     * @see io.netty.util.AttributeMap#attr(io.netty.util.AttributeKey)
     */
    public <T> Attribute<T> attr(AttributeKey<T> key) {
        return channel.attr(key);
    }


}
