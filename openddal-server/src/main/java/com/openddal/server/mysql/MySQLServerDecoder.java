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

import java.util.List;

import com.openddal.server.mysql.proto.Packet;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

/**
 * @author jorgie.li
 *
 */
public class MySQLServerDecoder extends ByteToMessageDecoder {

    private static final int FRAME_LENGTH_FIELD_LENGTH = 4;

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        // Make sure if the length field was received.
        if (in.readableBytes() < FRAME_LENGTH_FIELD_LENGTH) {
            // The length field was not received yet - return.
            // This method will be invoked again when more packets are
            // received and appended to the buffer.
            return;
        }
        // The length field is in the buffer.
        // Mark the current buffer position before reading the length field
        // because the whole frame might not be in the buffer yet.
        // We will reset the buffer position to the marked position if
        // there's not enough bytes in the buffer.
        in.markReaderIndex();
        
        int frameLength = readLength(in);// in.readInt();
        // Make sure if there's enough bytes in the buffer.
        if (in.readableBytes() < frameLength) {
            // The whole bytes were not received yet - return.
            // This method will be invoked again when more packets are
            // received and appended to the buffer.
            // Reset to the marked position to read the length field again
            // next time.
            in.resetReaderIndex();
            return;
        }
        // There's enough bytes in the buffer. Read it.
        ByteBuf frame = in.resetReaderIndex().readSlice(frameLength + 4).retain();
        // Successfully decoded a frame. Add the decoded frame.
        out.add(frame);
    }

    /**
     * @param in
     * @return
     */
    private int readLength(ByteBuf in) {
        byte[] lens = new byte[4];
        in.readBytes(lens);
        int size = Packet.getSize(lens);
        return size;
    }
}
