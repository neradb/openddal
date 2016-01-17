package com.openddal.server.mysql.proto.test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.openddal.server.mysql.proto.Com_Initdb;
import com.openddal.server.mysql.proto.Proto;

public class Com_InitdbTest {
    @Test
    public void test1() {
        byte[] packet = Proto.packet_string_to_bytes(""
            + "05 00 00 00 02 74 65 73    74"
        );

        Com_Initdb pkt = Com_Initdb.loadFromPacket(packet);
        assertArrayEquals(packet, pkt.toPacket());
        assertEquals(pkt.schema, "test");
    }
}
