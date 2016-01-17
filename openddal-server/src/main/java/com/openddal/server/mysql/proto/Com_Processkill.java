package com.openddal.server.mysql.proto;

import java.util.ArrayList;

public class Com_Processkill extends Packet {
    public long connectionId = 0;
    
    public ArrayList<byte[]> getPayload() {
        ArrayList<byte[]> payload = new ArrayList<byte[]>();
        
        payload.add(Proto.build_byte(Flags.COM_PROCESS_KILL));
        payload.add(Proto.build_fixed_int(4, this.connectionId));
        
        return payload;
    }
    
    public static Com_Processkill loadFromPacket(byte[] packet) {
        Com_Processkill obj = new Com_Processkill();
        Proto proto = new Proto(packet, 3);
        
        obj.sequenceId = proto.get_fixed_int(1);
        proto.get_filler(1);
        obj.connectionId = proto.get_fixed_int(4);
        
        return obj;
    }
}
