package com.openddal.server.mysql.proto;

import java.util.ArrayList;

public class Com_Statistics extends Packet {
    
    public ArrayList<byte[]> getPayload() {
        ArrayList<byte[]> payload = new ArrayList<byte[]>();
        
        payload.add(Proto.build_byte(Flags.COM_STATISTICS));
        
        return payload;
    }
    
    public static Com_Statistics loadFromPacket(byte[] packet) {
        Com_Statistics obj = new Com_Statistics();
        Proto proto = new Proto(packet, 3);
        
        obj.sequenceId = proto.get_fixed_int(1);
        
        return obj;
    }
}
