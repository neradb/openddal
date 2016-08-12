package com.openddal.server.mysql.proto.test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.openddal.server.mysql.proto.LocalInfileRequest;

public class Local_Infile_RequestTest {
    @Test
    public void test1() {
        byte[] packet = ProtoTest.packet_string_to_bytes(""
            + "0c 00 00 01 fb 2f 65 74    63 2f 70 61 73 73 77 64"
        );

        LocalInfileRequest pkt = LocalInfileRequest.loadFromPacket(packet);
        assertArrayEquals(packet, pkt.toPacket());
        assertEquals(pkt.filename, "/etc/passwd");
    }
}
