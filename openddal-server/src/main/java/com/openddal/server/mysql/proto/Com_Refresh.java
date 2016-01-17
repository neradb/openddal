package com.openddal.server.mysql.proto;

import java.util.ArrayList;

public class Com_Refresh extends Packet {
    public long flags = 0x00;
    
    public ArrayList<byte[]> getPayload() {
        ArrayList<byte[]> payload = new ArrayList<byte[]>();
        
        payload.add(Proto.build_byte(Flags.COM_REFRESH));
        payload.add(Proto.build_fixed_int(1, this.flags));
        
        return payload;
    }
    
    public static Com_Refresh loadFromPacket(byte[] packet) {
        Com_Refresh obj = new Com_Refresh();
        Proto proto = new Proto(packet, 3);
        
        obj.sequenceId = proto.get_fixed_int(1);
        proto.get_filler(1);
        obj.flags = proto.get_fixed_int(1);
        
        return obj;
    }
}
