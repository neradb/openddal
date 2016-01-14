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
package com.openddal.server.mysql.packet;

import com.openddal.server.util.BufferUtil;

import io.netty.buffer.ByteBuf;

/**
 * From client to server when the client do heartbeat between mycat cluster.
 * 
 * <pre>
 * Bytes Name ----- ---- 1 command n id
 * 
 * @author mycat
 */
public class HeartbeatPacket extends MySQLPacket {

    public byte command;
    public long id;

    public void read(byte[] data) {
        MySQLMessage mm = new MySQLMessage(data);
        packetLength = mm.readUB3();
        packetId = mm.read();
        command = mm.read();
        id = mm.readLength();
    }

    public void write(ByteBuf buffer) {
        BufferUtil.writeUB3(buffer, calcPacketSize());
        buffer.writeByte(packetId);
        buffer.writeByte(command);
        BufferUtil.writeLength(buffer, id);
    }

    @Override
    public int calcPacketSize() {
        return 1 + BufferUtil.getLength(id);
    }

    @Override
    protected String getPacketInfo() {
        return "Mycat Heartbeat Packet";
    }

}