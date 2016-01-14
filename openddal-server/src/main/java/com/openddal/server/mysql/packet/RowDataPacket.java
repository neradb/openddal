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

import java.util.ArrayList;
import java.util.List;

import com.openddal.server.util.BufferUtil;

import io.netty.buffer.ByteBuf;

/**
 * From server to client. One packet for each row in the result set.
 * 
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
 * 
 * @author mycat
 */
public class RowDataPacket extends MySQLPacket {
	private static final byte NULL_MARK = (byte) 251;
	private static final byte EMPTY_MARK = (byte) 0;
	public int fieldCount;
	public final List<byte[]> fieldValues;

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

	public void write(ByteBuf buffer) {
		int size = calcPacketSize();
		BufferUtil.writeUB3(buffer, size);
		buffer.writeByte(packetId);
		for (int i = 0; i < fieldCount; i++) {
			byte[] fv = fieldValues.get(i);
			if (fv == null) {
				buffer.writeByte(RowDataPacket.NULL_MARK);
			} else if (fv.length == 0) {
				buffer.writeByte(RowDataPacket.EMPTY_MARK);
			} else {
				BufferUtil.writeLength(buffer, fv.length);
				buffer.writeBytes(fv);
			}
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