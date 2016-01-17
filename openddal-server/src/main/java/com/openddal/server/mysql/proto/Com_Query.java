package com.openddal.server.mysql.proto;

import java.util.ArrayList;

public class Com_Query extends Packet {
    public String query = "";
    
    public ArrayList<byte[]> getPayload() {
        ArrayList<byte[]> payload = new ArrayList<byte[]>();
        
        payload.add(Proto.build_byte(Flags.COM_QUERY));
        payload.add(Proto.build_fixed_str(this.query.length(), this.query));
        
        return payload;
    }

    public static Com_Query loadFromPacket(byte[] packet) {
        Com_Query obj = new Com_Query();
        Proto proto = new Proto(packet, 3);
        
        obj.sequenceId = proto.get_fixed_int(1);
        proto.get_filler(1);
        obj.query = proto.get_eop_str();
        
        return obj;
    }
    
}
