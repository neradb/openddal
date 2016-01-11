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
import io.netty.channel.ChannelHandlerContext;
import org.apache.thrift.transport.TTransportException;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class ProtocolTransport {

    public static final int DEFAULT_BUFFER_SIZE = 1024;
    public ByteBuf in;
    public ByteBuf out;
    private ChannelHandlerContext ctx;

    public ProtocolTransport(ChannelHandlerContext ctx, ByteBuf in) {
        this.ctx = ctx;
        this.in = in;
        out = ctx.alloc().buffer(DEFAULT_BUFFER_SIZE);
    }

    public int read(byte[] bytes, int offset, int length) throws TTransportException {
        int _read = Math.min(in.readableBytes(), length);
        in.readBytes(bytes, offset, _read);
        return _read;
    }

    public void write(byte[] bytes, int offset, int length) throws TTransportException {
        out.writeBytes(bytes, offset, length);
    }

    /**
     * Writes the buffer to the output
     *
     * @param buf The output data buffer
     * @throws TTransportException if an error occurs writing data
     */
    public void write(byte[] buf) throws TTransportException {
        write(buf, 0, buf.length);
    }

    public ChannelHandlerContext getChannelHandlerContext() {
        return ctx;
    }

    public boolean isOpen() {
        return ctx.channel().isOpen();
    }

    public void close() {
        ctx.channel().close();
    }


}
