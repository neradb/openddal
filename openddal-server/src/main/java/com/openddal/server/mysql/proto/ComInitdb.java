package com.openddal.server.mysql.proto;

import java.util.ArrayList;

public class ComInitdb extends Packet {
    public String schema = "";
    
    public ArrayList<byte[]> getPayload() {
        ArrayList<byte[]> payload = new ArrayList<byte[]>();
        
        payload.add(Proto.build_byte(Flags.COM_INIT_DB));
        payload.add(Proto.build_fixed_str(this.schema.length(), this.schema));
        
        return payload;
    }
    
    public static ComInitdb loadFromPacket(byte[] packet) {
        ComInitdb obj = new ComInitdb();
        Proto proto = new Proto(packet, 3);
        
        obj.sequenceId = proto.get_fixed_int(1);
        proto.get_filler(1);
        obj.schema = proto.get_eop_str();
        
        return obj;
    }

    @Override
    public String toString() {
        return "COM_INIT_DB[schema=" + schema + "]";
    }
    
    
}
