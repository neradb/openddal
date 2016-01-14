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
 * From server to client after command, if no error and result set -- that is,
 * if the command was a query which returned a result set. The Result Set Header
 * Packet is the first of several, possibly many, packets that the server sends
 * for result sets. The order of packets for a result set is:
 * 
 * <pre>
 * (Result Set Header Packet)   the number of columns
 * (Field Packets)              column descriptors
 * (EOF Packet)                 marker: end of Field Packets
 * (Row Data Packets)           row contents
 * (EOF Packet)                 marker: end of Data Packets
 * 
 * Bytes                        Name
 * -----                        ----
 * 1-9   (Length-Coded-Binary)  field_count
 * 1-9   (Length-Coded-Binary)  extra
 * 
 * @see http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol#Result_Set_Header_Packet
 * </pre>
 * 
 * @author mycat
 */
public class ResultSetHeaderPacket extends MySQLPacket {

	public int fieldCount;
	public long extra;

	public void read(byte[] data) {
		MySQLMessage mm = new MySQLMessage(data);
		this.packetLength = mm.readUB3();
		this.packetId = mm.read();
		this.fieldCount = (int) mm.readLength();
		if (mm.hasRemaining()) {
			this.extra = mm.readLength();
		}
	}

	public void write(ByteBuf buffer) {
		int size = calcPacketSize();
		BufferUtil.writeUB3(buffer, size);
		buffer.writeByte(packetId);
		BufferUtil.writeLength(buffer, fieldCount);
		if (extra > 0) {
			BufferUtil.writeLength(buffer, extra);
		}
	}

	@Override
	public int calcPacketSize() {
		int size = BufferUtil.getLength(fieldCount);
		if (extra > 0) {
			size += BufferUtil.getLength(extra);
		}
		return size;
	}

	@Override
	protected String getPacketInfo() {
		return "MySQL ResultSetHeader Packet";
	}

}