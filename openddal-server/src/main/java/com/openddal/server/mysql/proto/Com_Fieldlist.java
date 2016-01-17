package com.openddal.server.mysql.proto;

import java.util.ArrayList;

public class Com_Fieldlist extends Packet {
    public String table = "";
    public String fields = "";
    
    public ArrayList<byte[]> getPayload() {
        ArrayList<byte[]> payload = new ArrayList<byte[]>();
        
        payload.add(Proto.build_byte(Flags.COM_FIELD_LIST));
        payload.add(Proto.build_null_str(this.table));
        payload.add(Proto.build_fixed_str(this.fields.length(), this.fields));
        
        return payload;
    }
    
    public static Com_Fieldlist loadFromPacket(byte[] packet) {
        Com_Fieldlist obj = new Com_Fieldlist();
        Proto proto = new Proto(packet, 3);
        
        obj.sequenceId = proto.get_fixed_int(1);
        proto.get_filler(1);
        obj.table = proto.get_null_str();
        obj.fields = proto.get_eop_str();
        
        return obj;
    }
}
