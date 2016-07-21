package com.openddal.server.mysql.proto.test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.openddal.server.mysql.proto.EOF;
import com.openddal.server.mysql.proto.Flags;

public class EOFTest {
    @Test
    public void test1() {
        byte[] packet = ProtoTest.packet_string_to_bytes(
            "05 00 00 05 fe 00 00 02 00"
        );

        EOF pkt = EOF.loadFromPacket(packet);
        assertArrayEquals(packet, pkt.toPacket());
        assertEquals(pkt.warnings, 0);
        assertEquals(pkt.hasStatusFlag(Flags.SERVER_STATUS_AUTOCOMMIT), true);
        assertEquals(pkt.hasStatusFlag(Flags.SERVER_QUERY_WAS_SLOW), false);
    }
}
