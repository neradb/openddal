package com.openddal.server.mysql.proto;

import java.util.ArrayList;

public class AuthMoreData extends Packet {
    public String pluginData = "";
    
    public ArrayList<byte[]> getPayload() {
        ArrayList<byte[]> payload = new ArrayList<byte[]>();
        
        payload.add(Proto.build_byte((byte)0x01));
        payload.add(Proto.build_eop_str(this.pluginData));
        
        return payload;
    }
    
    public static AuthMoreData loadFromPacket(byte[] packet) {
        AuthMoreData obj = new AuthMoreData();
        Proto proto = new Proto(packet, 3);
        
        obj.sequenceId = proto.get_fixed_int(1);
        proto.get_filler(1);
        obj.pluginData = proto.get_eop_str();
        
        return obj;
    }
}
