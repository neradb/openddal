package com.openddal.server.mysql.proto.test;

import static org.junit.Assert.assertArrayEquals;

import org.junit.Test;

import com.openddal.server.mysql.proto.ComQuit;

public class Com_QuitTest {
    @Test
    public void test1() {
        byte[] packet = ProtoTest.packet_string_to_bytes(""
            + "01 00 00 00 01"
        );

        ComQuit pkt = ComQuit.loadFromPacket(packet);
        assertArrayEquals(packet, pkt.toPacket());
    }
}
