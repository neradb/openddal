package com.openddal.server.mysql.proto;

import java.util.ArrayList;

public class Com_Setoption extends Packet {
    public long operation = 0;
    
    public ArrayList<byte[]> getPayload() {
        ArrayList<byte[]> payload = new ArrayList<byte[]>();
        
        payload.add(Proto.build_byte(Flags.COM_SET_OPTION));
        payload.add(Proto.build_fixed_int(2, this.operation));
        
        return payload;
    }

    public static Com_Setoption loadFromPacket(byte[] packet) {
        Com_Setoption obj = new Com_Setoption();
        Proto proto = new Proto(packet, 3);
        
        obj.sequenceId = proto.get_fixed_int(1);
        proto.get_filler(1);
        obj.operation = proto.get_fixed_int(2);
        
        return obj;
    }
    
}
