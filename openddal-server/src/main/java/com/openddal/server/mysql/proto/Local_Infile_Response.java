package com.openddal.server.mysql.proto;

import java.util.ArrayList;

public class Local_Infile_Response extends Packet {
    public byte[] data;
    
    public ArrayList<byte[]> getPayload() {
        ArrayList<byte[]> payload = new ArrayList<byte[]>();
        
        payload.add(this.data);
        
        return payload;
    }
    
    public static Local_Infile_Response loadFromPacket(byte[] packet) {
        Local_Infile_Response obj = new Local_Infile_Response();
        Proto proto = new Proto(packet, 3);
        
        obj.sequenceId = proto.get_fixed_int(1);
        
        int size = packet.length - proto.offset;
        obj.data = new byte[size];
        
        System.arraycopy(packet, proto.offset, obj.data, 0, size);

        return obj;
    }
}
