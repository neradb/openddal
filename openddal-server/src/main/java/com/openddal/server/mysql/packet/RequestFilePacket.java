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
 * load data local infile 向客户端请求发送文件用
 */
public class RequestFilePacket extends MySQLPacket {
    public static final byte FIELD_COUNT = (byte) 251;
    public byte command = FIELD_COUNT;
    public byte[] fileName;

    public void write(ByteBuf buffer) {
        int size = calcPacketSize();
        BufferUtil.writeUB3(buffer, size);
        buffer.writeByte(packetId);
        buffer.writeByte(command);
        if (fileName != null) {
            buffer.writeBytes(fileName);
        }
    }

    @Override
    public int calcPacketSize() {
        return fileName == null ? 1 : 1 + fileName.length;
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL Request File Packet";
    }

}