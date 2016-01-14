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

import java.io.IOException;
import java.io.InputStream;

import com.openddal.server.util.BufferUtil;
import com.openddal.server.util.StreamUtil;

import io.netty.buffer.ByteBuf;

/**
 * @author mycat
 */
public class BinaryPacket extends MySQLPacket {
	public static final byte OK = 1;
	public static final byte ERROR = 2;
	public static final byte HEADER = 3;
	public static final byte FIELD = 4;
	public static final byte FIELD_EOF = 5;
	public static final byte ROW = 6;
	public static final byte PACKET_EOF = 7;

	public byte[] data;

	public void read(InputStream in) throws IOException {
		packetLength = StreamUtil.readUB3(in);
		packetId = StreamUtil.read(in);
		byte[] ab = new byte[packetLength];
		StreamUtil.read(in, ab, 0, ab.length);
		data = ab;
	}
	
	public void write(ByteBuf buffer) {
        BufferUtil.writeUB3(buffer, calcPacketSize());
        buffer.writeByte(packetId);
        buffer.writeBytes(data);
    }

	@Override
	public int calcPacketSize() {
		return data == null ? 0 : data.length;
	}

	@Override
	protected String getPacketInfo() {
		return "MySQL Binary Packet";
	}

}