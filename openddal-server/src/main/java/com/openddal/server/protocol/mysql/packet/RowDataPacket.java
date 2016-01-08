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
import io.mycat.net.Connection;
import io.mycat.net.NetSystem;
import io.mycat.server.packet.util.BufferUtil;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * From server to client. One packet for each row in the result set.
 * <p>
 * <pre>
 * Bytes                   Name
 * -----                   ----
 * n (Length Coded String) (column value)
 * ...
 *
 * (column value):         The data in the column, as a character string.
 *                         If a column is defined as non-character, the
 *                         server converts the value into a character
 *                         before sending it. Since the value is a Length
 *                         Coded String, a NULL can be represented with a
 *                         single byte containing 251(see the description
 *                         of Length Coded Strings in section "Elements" above).
 *
 * @see http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol#Row_Data_Packet
 * </pre>
 */
public class RowDataPacket extends MySQLPacket {
    private static final byte NULL_MARK = (byte) 251;
    private static final byte EMPTY_MARK = (byte) 0;
    public final List<byte[]> fieldValues;
    public int fieldCount;

    public RowDataPacket(int fieldCount) {
        this.fieldCount = fieldCount;
        this.fieldValues = new ArrayList<byte[]>(fieldCount);
    }

    public void add(byte[] value) {
        // 这里应该修改value
        fieldValues.add(value);
    }

    public void addFieldCount(int add) {
        // 这里应该修改field
        fieldCount = fieldCount + add;
    }

    public void read(byte[] data) {
        MySQLMessage mm = new MySQLMessage(data);
        packetLength = mm.readUB3();
        packetId = mm.read();
        for (int i = 0; i < fieldCount; i++) {
            fieldValues.add(mm.readBytesWithLength());
        }
    }

    @Override
    public void write(BufferArray bufferArray) {
        int size = calcPacketSize();
        ByteBuffer buffer = bufferArray.checkWriteBuffer(packetHeaderSize
                + size);
        BufferUtil.writeUB3(buffer, size);
        buffer.put(packetId);
        for (int i = 0; i < fieldCount; i++) {
            byte[] fv = fieldValues.get(i);
            if (fv == null) {
                buffer = bufferArray.checkWriteBuffer(1);
                buffer.put(RowDataPacket.NULL_MARK);
            } else if (fv.length == 0) {
                buffer = bufferArray.checkWriteBuffer(1);
                buffer.put(RowDataPacket.EMPTY_MARK);
            } else {
                buffer = bufferArray.checkWriteBuffer(BufferUtil
                        .getLength(fv.length));
                BufferUtil.writeLength(buffer, fv.length);
                bufferArray.write(fv);
            }
        }
    }

    public void write(Connection conn) {
        int size = calcPacketSize();
        int totalSize = size + packetHeaderSize;
        if (NetSystem.getInstance().getBufferPool().getChunkSize() >= totalSize) {
            ByteBuffer buffer = NetSystem.getInstance().getBufferPool()
                    .allocate();
            BufferUtil.writeUB3(buffer, size);
            buffer.put(packetId);
            for (int i = 0; i < fieldCount; i++) {
                byte[] fv = fieldValues.get(i);
                if (fv == null) {
                    buffer.put(RowDataPacket.NULL_MARK);
                } else if (fv.length == 0) {
                    buffer.put(RowDataPacket.EMPTY_MARK);
                } else {
                    BufferUtil.writeLength(buffer, fv.length);
                    buffer.put(fv);
                }
            }
            conn.write(buffer);
        } else {
            BufferArray bufArray = NetSystem.getInstance().getBufferPool()
                    .allocateArray();
            write(bufArray);
            conn.write(bufArray);
        }

    }

    @Override
    public int calcPacketSize() {
        int size = 0;
        for (int i = 0; i < fieldCount; i++) {
            byte[] v = fieldValues.get(i);
            size += (v == null || v.length == 0) ? 1 : BufferUtil.getLength(v);
        }
        return size;
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL RowData Packet";
    }

}