package com.openddal.server.mysql.proto.test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.openddal.server.mysql.proto.ComInitdb;

public class Com_InitdbTest {
    @Test
    public void test1() {
        byte[] packet = ProtoTest.packet_string_to_bytes(""
            + "05 00 00 00 02 74 65 73    74"
        );

        ComInitdb pkt = ComInitdb.loadFromPacket(packet);
        assertArrayEquals(packet, pkt.toPacket());
        assertEquals(pkt.schema, "test");
    }
}
