package com.openddal.server.mysql.proto.test;

import static org.junit.Assert.assertArrayEquals;

import org.junit.Test;

import com.openddal.server.mysql.proto.Local_Infile_Response;

public class Local_Infile_ResponseTest {
    @Test
    public void test1() {
        byte[] packet = ProtoTest.packet_string_to_bytes(""
            + "05 00 00 00 00 FF FE FD FC"
        );

        Local_Infile_Response pkt = Local_Infile_Response.loadFromPacket(packet);
        
        assertArrayEquals(packet, pkt.toPacket());
    }
}
