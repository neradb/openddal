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

import java.io.IOException;
import java.io.OutputStream;

import com.openddal.server.util.BufferUtil;
import com.openddal.server.util.Capabilities;
import com.openddal.server.util.StreamUtil;

import io.netty.buffer.ByteBuf;

/**
 * From client to server during initial handshake.
 * 
 * <pre>
 * Bytes                        Name
 * -----                        ----
 * 4                            client_flags
 * 4                            max_packet_size
 * 1                            charset_number
 * 23                           (filler) always 0x00...
 * n (Null-Terminated String)   user
 * n (Length Coded Binary)      scramble_buff (1 + x bytes)
 * n (Null-Terminated String)   databasename (optional)
 * 
 * @see http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol#Client_Authentication_Packet
 * </pre>
 * 
 */
public class AuthPacket extends MySQLPacket {
	private static final byte[] FILLER = new byte[23];

	public long clientFlags;
	public long maxPacketSize;
	public int charsetIndex;
	public byte[] extra;// from FILLER(23)
	public String user;
	public byte[] password;
	public String database;

	public void read(byte[] data) {
		MySQLMessage mm = new MySQLMessage(data);
		packetLength = mm.readUB3();
		packetId = mm.read();
		clientFlags = mm.readUB4();
		maxPacketSize = mm.readUB4();
		charsetIndex = (mm.read() & 0xff);
		// read extra
		int current = mm.position();
		int len = (int) mm.readLength();
		if (len > 0 && len < FILLER.length) {
			byte[] ab = new byte[len];
			System.arraycopy(mm.bytes(), mm.position(), ab, 0, len);
			this.extra = ab;
		}
		mm.position(current + FILLER.length);
		user = mm.readStringWithNull();
		password = mm.readBytesWithLength();
		if (((clientFlags & Capabilities.CLIENT_CONNECT_WITH_DB) != 0)
				&& mm.hasRemaining()) {
			database = mm.readStringWithNull();
		}
	}

	public void write(OutputStream out) throws IOException {
		StreamUtil.writeUB3(out, calcPacketSize());
		StreamUtil.write(out, packetId);
		StreamUtil.writeUB4(out, clientFlags);
		StreamUtil.writeUB4(out, maxPacketSize);
		StreamUtil.write(out, (byte) charsetIndex);
		out.write(FILLER);
		if (user == null) {
			StreamUtil.write(out, (byte) 0);
		} else {
			StreamUtil.writeWithNull(out, user.getBytes());
		}
		if (password == null) {
			StreamUtil.write(out, (byte) 0);
		} else {
			StreamUtil.writeWithLength(out, password);
		}
		if (database == null) {
			StreamUtil.write(out, (byte) 0);
		} else {
			StreamUtil.writeWithNull(out, database.getBytes());
		}
	}
	
    public void write(ByteBuf buffer) {
        BufferUtil.writeUB3(buffer, calcPacketSize());
        buffer.writeByte(packetId);
        BufferUtil.writeUB4(buffer, clientFlags);
        BufferUtil.writeUB4(buffer, maxPacketSize);
        buffer.writeByte((byte) charsetIndex);
        buffer.writeBytes(FILLER);
       
        if (user == null) {
            buffer.writeByte((byte) 0);
        } else {
            byte[] userData = user.getBytes();
            BufferUtil.writeWithNull(buffer, userData);
        }
        if (password == null) {
            buffer.writeByte((byte) 0);
        } else {
            BufferUtil.writeWithLength(buffer, password);
        }
        if (database == null) {
            buffer.writeByte((byte) 0);
        } else {
            byte[] databaseData = database.getBytes();
            BufferUtil.writeWithNull(buffer, databaseData);
        }
    }

	@Override
	public int calcPacketSize() {
		int size = 32;// 4+4+1+23;
		size += (user == null) ? 1 : user.length() + 1;
		size += (password == null) ? 1 : BufferUtil.getLength(password);
		size += (database == null) ? 1 : database.length() + 1;
		return size;
	}

	@Override
	protected String getPacketInfo() {
		return "MySQL Authentication Packet";
	}

}