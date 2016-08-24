package com.openddal.server.mysql.proto;

import java.util.ArrayList;

public class ComQuit extends Packet {
    
    public ArrayList<byte[]> getPayload() {
        ArrayList<byte[]> payload = new ArrayList<byte[]>();
        
        payload.add(Proto.build_byte(Flags.COM_QUIT));
        
        return payload;
    }
    
    public static ComQuit loadFromPacket(byte[] packet) {
        ComQuit obj = new ComQuit();
        Proto proto = new Proto(packet, 3);
        
        obj.sequenceId = proto.get_fixed_int(1);
        proto.get_filler(1);
        
        return obj;
    }

    @Override
    public String toString() {
        return "COM_QUIT";
    }
    
}
