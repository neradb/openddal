package com.openddal.server.mysql.proto;

import java.util.ArrayList;

public class Local_Infile_Request extends Packet {
    public String filename = "";
    
    public ArrayList<byte[]> getPayload() {
        ArrayList<byte[]> payload = new ArrayList<byte[]>();
        
        payload.add(Proto.build_byte(Flags.LOCAL_INFILE));
        payload.add( Proto.build_eop_str(this.filename));
        
        return payload;
    }
    
    public static Local_Infile_Request loadFromPacket(byte[] packet) {
        Local_Infile_Request obj = new Local_Infile_Request();
        Proto proto = new Proto(packet, 3);
        
        obj.sequenceId = proto.get_fixed_int(1);
        proto.get_filler(1);
        obj.filename = proto.get_eop_str();

        return obj;
    }
}
