package com.openddal.server.mysql.proto.test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.openddal.server.mysql.proto.ColCountPacket;

public class ColCountTest {
    @Test
    public void test1() {
        byte[] packet = new byte[] {
            (byte)0x01, (byte)0x00, (byte)0x00, (byte)0x01, (byte)0x05
        };

        ColCountPacket colcount = ColCountPacket.loadFromPacket(packet);

        assertArrayEquals(packet, colcount.toPacket());
        assertEquals(colcount.colCount, 5);
        assertEquals(colcount.sequenceId, 1);
    }
}
