package com.openddal.server.mysql.proto;

import java.util.ArrayList;

public class Com_Debug extends Packet {
    public ArrayList<byte[]> getPayload() {
        ArrayList<byte[]> payload = new ArrayList<byte[]>();
        
        payload.add(Proto.build_byte(Flags.COM_DEBUG));
        
        return payload;
    }
    
    public static Com_Debug loadFromPacket(byte[] packet) {
        Com_Debug obj = new Com_Debug();
        Proto proto = new Proto(packet, 3);
        
        obj.sequenceId = proto.get_fixed_int(1);
        
        return obj;
    }
}
