package com.openddal.server.mysql.proto.test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.openddal.server.mysql.proto.ComQuery;

public class Com_QueryTest {
    @Test
    public void test1() {
        byte[] packet = ProtoTest.packet_string_to_bytes(""
            + "2e 00 00 00 03 73 65 6c    65 63 74 20 22 30 31 32"
            + "33 34 35 36 37 38 39 30    31 32 33 34 35 36 37 38"
            + "39 30 31 32 33 34 35 36    37 38 39 30 31 32 33 34"
            + "35 22                                             "
        );

        ComQuery pkt = ComQuery.loadFromPacket(packet);
        assertArrayEquals(packet, pkt.toPacket());
        assertEquals(pkt.query, "select \"012345678901234567890123456789012345\"");
    }
    
    @Test
    public void test2() {
        byte[] packet = ProtoTest.packet_string_to_bytes(""
            + "09 00 00 00 03 53 45 4c 45 43 54 20 31"
        );

        ComQuery pkt = ComQuery.loadFromPacket(packet);
        assertArrayEquals(packet, pkt.toPacket());
        assertEquals(pkt.query, "SELECT 1");
    }
    
    @Test
    public void test3() {
        byte[] packet = ProtoTest.packet_string_to_bytes(""
            + "21 00 00 00 03 73 65 6c    65 63 74 20 40 40 76 65"
            + "72 73 69 6f 6e 5f 63 6f    6d 6d 65 6e 74 20 6c 69"
            + "6d 69 74 20 31                                    "
        );

        ComQuery pkt = ComQuery.loadFromPacket(packet);
        assertArrayEquals(packet, pkt.toPacket());
        assertEquals(pkt.query, "select @@version_comment limit 1");
    }
}
