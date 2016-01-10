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
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

/**
 * 
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 *
 */
public class ProtocolDecoder extends ByteToMessageDecoder {

    private static final int FRAME_LENGTH_FIELD_LENGTH = 4;

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        if (in.readableBytes() < FRAME_LENGTH_FIELD_LENGTH) {
            return;
        }

        in.markReaderIndex();
        int frameLength = in.readInt();

        if (in.readableBytes() < frameLength) {
            in.resetReaderIndex();
            return;
        }

        ByteBuf frame = in.readSlice(frameLength).retain();
        out.add(frame);
    }
}
