/*
 * Copyright 2014-2016 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the 鈥淟icense鈥�);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an 鈥淎S IS鈥� BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.openddal.server.protocol.mysql.packet;

import io.mycat.net.NetSystem;
import io.mycat.server.MySQLFrontConnection;
import io.mycat.server.packet.util.BufferUtil;

import java.nio.ByteBuffer;

/**
 * load data local infile 向客户端请求发送文件用
 */
public class RequestFilePacket extends MySQLPacket {
    public static final byte FIELD_COUNT = (byte) 251;
    public byte command = FIELD_COUNT;
    public byte[] fileName;


    public void write(MySQLFrontConnection c) {
        int size = calcPacketSize();
        ByteBuffer buffer = NetSystem.getInstance().getBufferPool()
                .allocate();
        BufferUtil.writeUB3(buffer, size);
        buffer.put(packetId);
        buffer.put(command);
        if (fileName != null) {

            buffer.put(fileName);

        }

        c.write(buffer);
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