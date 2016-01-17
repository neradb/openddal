package com.openddal.server.mysql.proto.test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.openddal.server.mysql.proto.AuthSwitchRequest;
import com.openddal.server.mysql.proto.Proto;

public class AuthSwitchRequestTest {
    @Test
    public void test1() {
        byte[] packet = Proto.packet_string_to_bytes(""
            + "2c 00 00 02 fe 6d 79 73    71 6c 5f 6e 61 74 69 76"
            + "65 5f 70 61 73 73 77 6f    72 64 00 7a 51 67 34 69"
            + "36 6f 4e 79 36 3d 72 48    4e 2f 3e 2d 62 29 41 00"
        );

        AuthSwitchRequest pkt = AuthSwitchRequest.loadFromPacket(packet);
        assertArrayEquals(packet, pkt.toPacket());
        assertEquals(pkt.pluginName, "mysql_native_password");
        assertEquals(pkt.authPluginData, "zQg4i6oNy6=rHN/>-b)A");
    }
}
