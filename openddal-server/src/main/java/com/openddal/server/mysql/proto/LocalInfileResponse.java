package com.openddal.server.mysql.proto;

import java.util.ArrayList;

public class LocalInfileResponse extends Packet {
    public byte[] data;
    
    public ArrayList<byte[]> getPayload() {
        ArrayList<byte[]> payload = new ArrayList<byte[]>();
        
        payload.add(this.data);
        
        return payload;
    }
    
    public static LocalInfileResponse loadFromPacket(byte[] packet) {
        LocalInfileResponse obj = new LocalInfileResponse();
        Proto proto = new Proto(packet, 3);
        
        obj.sequenceId = proto.get_fixed_int(1);
        
        int size = packet.length - proto.offset;
        obj.data = new byte[size];
        
        System.arraycopy(packet, proto.offset, obj.data, 0, size);

        return obj;
    }
}
