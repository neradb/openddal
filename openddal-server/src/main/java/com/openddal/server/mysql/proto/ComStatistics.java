package com.openddal.server.mysql.proto;

import java.util.ArrayList;

public class ComStatistics extends Packet {
    
    public ArrayList<byte[]> getPayload() {
        ArrayList<byte[]> payload = new ArrayList<byte[]>();
        
        payload.add(Proto.build_byte(Flags.COM_STATISTICS));
        
        return payload;
    }
    
    public static ComStatistics loadFromPacket(byte[] packet) {
        ComStatistics obj = new ComStatistics();
        Proto proto = new Proto(packet, 3);
        
        obj.sequenceId = proto.get_fixed_int(1);
        
        return obj;
    }

    @Override
    public String toString() {
        return "COM_STATISTICS";
    }
    
    
}
