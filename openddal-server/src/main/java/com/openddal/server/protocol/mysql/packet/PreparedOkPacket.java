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

import io.mycat.net.BufferArray;
import io.mycat.net.NetSystem;
import io.mycat.server.GenalMySQLConnection;
import io.mycat.server.packet.util.BufferUtil;

import java.nio.ByteBuffer;

/**
 * <pre>
 * From server to client, in response to prepared statement initialization packet.
 * It is made up of:
 *   1.a PREPARE_OK packet
 *   2.if "number of parameters" > 0
 *       (field packets) as in a Result Set Header Packet
 *       (EOF packet)
 *   3.if "number of columns" > 0
 *       (field packets) as in a Result Set Header Packet
 *       (EOF packet)
 *
 * -----------------------------------------------------------------------------------------
 *
 *  Bytes              Name
 *  -----              ----
 *  1                  0 - marker for OK packet
 *  4                  statement_handler_id
 *  2                  number of columns in result set
 *  2                  number of parameters in query
 *  1                  filler (always 0)
 *  2                  warning count
 *
 *  @see http://dev.mysql.com/doc/internals/en/prepared-statement-initialization-packet.html
 * </pre>
 */
public class PreparedOkPacket extends MySQLPacket {

    public byte flag;
    public long statementId;
    public int columnsNumber;
    public int parametersNumber;
    public byte filler;
    public int warningCount;

    public PreparedOkPacket() {
        this.flag = 0;
        this.filler = 0;
    }

    public void write(GenalMySQLConnection c) {
        ByteBuffer buffer = NetSystem.getInstance().getBufferPool().allocate();
        int size = calcPacketSize();
        BufferUtil.writeUB3(buffer, size);
        buffer.put(packetId);
        buffer.put(flag);
        BufferUtil.writeUB4(buffer, statementId);
        BufferUtil.writeUB2(buffer, columnsNumber);
        BufferUtil.writeUB2(buffer, parametersNumber);
        buffer.put(filler);
        BufferUtil.writeUB2(buffer, warningCount);
        c.write(buffer);
    }

    public void write(BufferArray bufferArray) {
        int size = calcPacketSize();
        int totalSize = size + packetHeaderSize;
        ByteBuffer buffer = bufferArray.checkWriteBuffer(totalSize);
        BufferUtil.writeUB3(buffer, size);
        buffer.put(packetId);
        buffer.put(flag);
        BufferUtil.writeUB4(buffer, statementId);
        BufferUtil.writeUB2(buffer, columnsNumber);
        BufferUtil.writeUB2(buffer, parametersNumber);
        buffer.put(filler);
        BufferUtil.writeUB2(buffer, warningCount);
    }

    @Override
    public int calcPacketSize() {
        return 12;
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL PreparedOk Packet";
    }

}