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
package com.openddal.server.packet;

import java.util.ArrayList;
import java.util.List;

import com.openddal.server.util.BufferUtil;
import com.openddal.server.util.Fields;

import io.netty.buffer.ByteBuf;

/**
 * ProtocolBinary::ResultsetRow: row of a binary resultset (COM_STMT_EXECUTE)
 * 
 * Payload 1 packet header [00] string[$len] NULL-bitmap, length: (column_count
 * + 7 + 2) / 8 string[$len] values
 * 
 * A Binary Protocol Resultset Row is made up of the NULL bitmap containing as
 * many bits as we have columns in the resultset + 2 and the values for columns
 * that are not NULL in the Binary Protocol Value format.
 * 
 * @see http://dev.mysql.com/doc/internals/en/binary-protocol-resultset-row.html
 *      #packet-ProtocolBinary::ResultsetRow
 * @see http://dev.mysql.com/doc/internals/en/binary-protocol-value.html
 * 
 */
public class BinaryRowDataPacket extends MySQLPacket {

    public int fieldCount;
    public List<byte[]> fieldValues;
    public byte packetHeader = (byte) 0;
    public byte[] nullBitMap;

    public List<FieldPacket> fieldPackets;

    public BinaryRowDataPacket() {
    }

    /**
     * 从RowDataPacket转换成BinaryRowDataPacket
     * 
     * @param fieldPackets 字段包集合
     * @param rowDataPk 文本协议行数据包
     */
    public void read(List<FieldPacket> fieldPackets, RowDataPacket rowDataPk) {
        this.fieldPackets = fieldPackets;
        this.fieldCount = rowDataPk.fieldCount;
        this.fieldValues = new ArrayList<byte[]>(fieldCount);
        nullBitMap = new byte[(fieldCount + 7 + 2) / 8];
        List<byte[]> _fieldValues = rowDataPk.fieldValues;
        for (int i = 0; i < fieldCount; i++) {
            byte[] fv = _fieldValues.get(i);
            if (fv == null) { // 字段值为null,根据协议规定存储nullBitMap
                int bitMapPos = (i + 2) / 8;
                int bitPos = (i + 2) % 8;
                nullBitMap[bitMapPos] |= (byte) (1 << bitPos);
                this.fieldValues.add(fv);
            } else {
                this.fieldValues.add(_fieldValues.get(i));
            }
        }
    }

    public void write(ByteBuf buffer) {
        BufferUtil.writeUB3(buffer, calcPacketSize());
        buffer.writeByte(packetId);
        buffer.writeByte(packetHeader); // packet header [00]
        buffer.writeBytes(nullBitMap); // NULL-Bitmap
        for (int i = 0; i < fieldCount; i++) { // values
            byte[] fv = fieldValues.get(i);
            if (fv != null) {
                FieldPacket fieldPk = this.fieldPackets.get(i);
                int fieldType = fieldPk.type;
                switch (fieldType) {
                case Fields.FIELD_TYPE_STRING:
                case Fields.FIELD_TYPE_VARCHAR:
                case Fields.FIELD_TYPE_VAR_STRING:
                case Fields.FIELD_TYPE_ENUM:
                case Fields.FIELD_TYPE_SET:
                case Fields.FIELD_TYPE_LONG_BLOB:
                case Fields.FIELD_TYPE_MEDIUM_BLOB:
                case Fields.FIELD_TYPE_BLOB:
                case Fields.FIELD_TYPE_TINY_BLOB:
                case Fields.FIELD_TYPE_GEOMETRY:
                case Fields.FIELD_TYPE_BIT:
                case Fields.FIELD_TYPE_DECIMAL:
                case Fields.FIELD_TYPE_NEW_DECIMAL:
                    // 长度编码的字符串需要一个字节来存储长度(0表示空字符串)
                    BufferUtil.writeLength(buffer, fv.length);
                    break;
                default:
                    break;
                }
                if (fv.length > 0) {
                    buffer.writeBytes(fv);
                }
            }
        }
    }

    @Override
    public int calcPacketSize() {
        int size = 0;
        size = size + 1 + nullBitMap.length;
        for (int i = 0, n = fieldValues.size(); i < n; i++) {
            byte[] value = fieldValues.get(i);
            if (value != null) {
                FieldPacket fieldPk = this.fieldPackets.get(i);
                int fieldType = fieldPk.type;
                switch (fieldType) {
                case Fields.FIELD_TYPE_STRING:
                case Fields.FIELD_TYPE_VARCHAR:
                case Fields.FIELD_TYPE_VAR_STRING:
                case Fields.FIELD_TYPE_ENUM:
                case Fields.FIELD_TYPE_SET:
                case Fields.FIELD_TYPE_LONG_BLOB:
                case Fields.FIELD_TYPE_MEDIUM_BLOB:
                case Fields.FIELD_TYPE_BLOB:
                case Fields.FIELD_TYPE_TINY_BLOB:
                case Fields.FIELD_TYPE_GEOMETRY:
                case Fields.FIELD_TYPE_BIT:
                case Fields.FIELD_TYPE_DECIMAL:
                case Fields.FIELD_TYPE_NEW_DECIMAL:
                    // 长度编码的字符串需要一个字节来存储长度
                    if (value.length != 0) {
                        size = size + 1 + value.length;
                    } else {
                        size = size + 1; // 处理空字符串,只计算长度1个字节
                    }
                    break;
                default:
                    size = size + value.length;
                    break;
                }
            }
        }
        return size;
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL Binary RowData Packet";
    }

}
