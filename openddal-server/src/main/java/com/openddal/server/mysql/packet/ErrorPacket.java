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

import java.nio.ByteBuffer;

import com.openddal.server.util.BufferUtil;

import io.netty.buffer.ByteBuf;

/**
 * From server to client in response to command, if error.
 * 
 * <pre>
 * Bytes                       Name
 * -----                       ----
 * 1                           field_count, always = 0xff
 * 2                           errno
 * 1                           (sqlstate marker), always '#'
 * 5                           sqlstate (5 characters)
 * n                           message
 * 
 * @see http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol#Error_Packet
 * </pre>
 * 
 * @author mycat
 */
public class ErrorPacket extends MySQLPacket {
	public static final byte FIELD_COUNT = (byte) 0xff;
	private static final byte SQLSTATE_MARKER = (byte) '#';
	private static final byte[] DEFAULT_SQLSTATE = "HY000".getBytes();

	public byte fieldCount = FIELD_COUNT;
	public int errno;
	public byte mark = SQLSTATE_MARKER;
	public byte[] sqlState = DEFAULT_SQLSTATE;
	public byte[] message;

	public void read(BinaryPacket bin) {
		packetLength = bin.packetLength;
		packetId = bin.packetId;
		MySQLMessage mm = new MySQLMessage(bin.data);
		fieldCount = mm.read();
		errno = mm.readUB2();
		if (mm.hasRemaining() && (mm.read(mm.position()) == SQLSTATE_MARKER)) {
			mm.read();
			sqlState = mm.readBytes(5);
		}
		message = mm.readBytes();
	}

	public void read(byte[] data) {
		MySQLMessage mm = new MySQLMessage(data);
		packetLength = mm.readUB3();
		packetId = mm.read();
		fieldCount = mm.read();
		errno = mm.readUB2();
		if (mm.hasRemaining() && (mm.read(mm.position()) == SQLSTATE_MARKER)) {
			mm.read();
			sqlState = mm.readBytes(5);
		}
		message = mm.readBytes();
	}

	public byte[] writeToBytes() {
		ByteBuffer buffer = ByteBuffer.allocate(calcPacketSize() + 4);
		int size = calcPacketSize();
		BufferUtil.writeUB3(buffer, size);
		buffer.put(packetId);
		buffer.put(fieldCount);
		BufferUtil.writeUB2(buffer, errno);
		buffer.put(mark);
		buffer.put(sqlState);
		if (message != null) {
			buffer.put(message);
		}
		buffer.flip();
		byte[] data = new byte[buffer.limit()];
		buffer.get(data);

		return data;
	}

	public void write(ByteBuf buffer) {
		BufferUtil.writeUB3(buffer, calcPacketSize());
		buffer.writeByte(packetId);
		buffer.writeByte(fieldCount);
		BufferUtil.writeUB2(buffer, errno);
		buffer.writeByte(mark);
		buffer.writeBytes(sqlState);
		if (message != null) {
			buffer.writeBytes(message);

		}
	}

	@Override
	public int calcPacketSize() {
		int size = 9;// 1 + 2 + 1 + 5
		if (message != null) {
			size += message.length;
		}
		return size;
	}

	@Override
	protected String getPacketInfo() {
		return "MySQL Error Packet";
	}

}