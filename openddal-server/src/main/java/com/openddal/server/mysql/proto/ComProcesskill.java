package com.openddal.server.mysql.proto;

import java.util.ArrayList;

public class ComProcesskill extends Packet {
    public long connectionId = 0;
    
    public ArrayList<byte[]> getPayload() {
        ArrayList<byte[]> payload = new ArrayList<byte[]>();
        
        payload.add(Proto.build_byte(Flags.COM_PROCESS_KILL));
        payload.add(Proto.build_fixed_int(4, this.connectionId));
        
        return payload;
    }
    
    public static ComProcesskill loadFromPacket(byte[] packet) {
        ComProcesskill obj = new ComProcesskill();
        Proto proto = new Proto(packet, 3);
        
        obj.sequenceId = proto.get_fixed_int(1);
        proto.get_filler(1);
        obj.connectionId = proto.get_fixed_int(4);
        
        return obj;
    }

    @Override
    public String toString() {
        return "COM_PROCESS_KILL[connectionId=" + connectionId + "]";
    }
    
    
}
