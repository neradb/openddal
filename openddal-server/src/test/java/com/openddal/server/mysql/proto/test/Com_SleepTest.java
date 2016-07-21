package com.openddal.server.mysql.proto.test;

import static org.junit.Assert.assertArrayEquals;

import org.junit.Test;

import com.openddal.server.mysql.proto.Com_Sleep;

public class Com_SleepTest {
    @Test
    public void test1() {
        byte[] packet = ProtoTest.packet_string_to_bytes(""
            + "01 00 00 00 00"
        );

        Com_Sleep pkt = Com_Sleep.loadFromPacket(packet);
        assertArrayEquals(packet, pkt.toPacket());
    }
}
