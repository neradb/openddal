package com.openddal.server.mysql.proto.test;

import static org.junit.Assert.assertArrayEquals;

import org.junit.Test;

import com.openddal.server.mysql.proto.OldAuthSwitchRequest;
import com.openddal.server.mysql.proto.Proto;

public class OldAuthSwitchRequestTest {
    @Test
    public void test1() {
        byte[] packet = Proto.packet_string_to_bytes(""
            + "01 00 00 02 fe"
        );

        OldAuthSwitchRequest pkt = OldAuthSwitchRequest.loadFromPacket(packet);
        assertArrayEquals(packet, pkt.toPacket());
    }
}
