/*-------------------------------------------------------------------------------------------------
 _______ __   _ _______ _______ ______  ______
 |_____| | \  |    |    |______ |     \ |_____]
 |     | |  \_|    |    ______| |_____/ |_____]

 Copyright (c) 2016, antsdb.com and/or its affiliates. All rights reserved. *-xguo0<@

 This program is free software: you can redistribute it and/or modify it under the terms of the
 GNU Affero General Public License, version 3, as published by the Free Software Foundation.

 You should have received a copy of the GNU Affero General Public License along with this program.
 If not, see <https://www.gnu.org/licenses/agpl-3.0.txt>
-------------------------------------------------------------------------------------------------*/
package com.openddal.server.mysql;

import java.io.UnsupportedEncodingException;
import java.util.List;

import com.openddal.server.mysql.proto.Flags;
import com.openddal.server.mysql.proto.Packet;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

class PacketDecoder extends ByteToMessageDecoder {
    static final int COMMAND_HANDSKAE = -1; // mysql doesn't have this code

    boolean isHandshaken = false;
    MysqlServerHandler handler;

    public PacketDecoder(MysqlServerHandler handler) {
        super();
        this.handler = handler;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        // do we have length field in buffer ?

        if (!in.isReadable(4)) {
            return;
        }

        // do we have entire packet in the buffer?

        in.markReaderIndex();
        int size = BufferUtils.readLongInt(in) + 1;
        if (size == (0x00ffffff + 1)) {
            out.add(new ShutdownPacket());
            return;
        }
        if (!in.isReadable(size)) {
            in.resetReaderIndex();
            return;
        }

        int pos = in.readerIndex();
        try {
            Packet packet = readPacket(in, size - 2);
            out.add(packet);
        } finally {
            int currentPos = in.readerIndex();
            in.skipBytes(size - (currentPos - pos));
        }
    }

    private Packet readPacket(ByteBuf in, int size) {
        // packet sequence number for multiple packets

        // byte packetSequence = in.readByte();
        in.readByte();

        // handshake

        Packet packet = null;
        try {
            if (!this.isHandshaken) {
                this.isHandshaken = true;
                packet = new AuthPacket();
                packet.packetLength = size;
                packet.read(this.handler, in);
            } else {
                // command

                byte command = in.readByte();
                switch (command) {
                case Flags.COM_QUERY:
                    packet = new QueryPacket(command);
                    break;
                case Flags.COM_STMT_PREPARE:
                    packet = new StmtPreparePacket(command);
                    break;
                case Flags.COM_STMT_EXECUTE:
                    packet = new StmtExecutePacket(command);
                    break;
                case Flags.COM_PING:
                    packet = new PingPacket(command);
                    break;
                case Flags.COM_INIT_DB:
                    // Handle init_db cmd as query. Conversion is in QueryPacket
                    // read()
                    packet = new InitPacket(command);
                    break;
                case Flags.COM_STMT_CLOSE:
                    packet = new StmtClosePacket(command);
                    break;
                case Flags.COM_QUIT:
                    packet = new ClosePacket(command);
                    break;
                case Flags.COM_STMT_SEND_LONG_DATA:
                    packet = new LongDataPacket(command);
                    break;
                case Flags.COM_FIELD_LIST:
                    packet = new FieldListPacket(command);
                    break;
                case Flags.COM_SET_OPTION:
                    packet = new SetOptionPacket(command);
                    break;
                case Flags.COM_STMT_RESET:
                default:
                    throw new CodingError("unknown command: " + command);
                }
                packet.packetLength = size;
                packet.read(this.handler, in);
            }
        } catch (UnsupportedEncodingException e) {
            throw new CodingError("unknown command: " + e);
        }
        return packet;
    }

}
