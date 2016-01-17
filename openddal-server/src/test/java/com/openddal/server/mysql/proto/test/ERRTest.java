package com.openddal.server.mysql.proto.test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.openddal.server.mysql.proto.ERR;
import com.openddal.server.mysql.proto.Proto;

public class ERRTest {
    @Test
    public void test1() {
        byte[] packet = Proto.packet_string_to_bytes(
              "17 00 00 01 ff 48 04 23    48 59 30 30 30 4e 6f 20"
            + "74 61 62 6c 65 73 20 75    73 65 64"
        );

        ERR pkt = ERR.loadFromPacket(packet);

        assertArrayEquals(packet, pkt.toPacket());
        assertEquals(pkt.errorCode, 1096);
        assertEquals(pkt.sqlState, "HY000");
        assertEquals(pkt.errorMessage, "No tables used");
    }
}
