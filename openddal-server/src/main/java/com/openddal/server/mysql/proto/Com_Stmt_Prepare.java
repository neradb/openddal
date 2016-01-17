package com.openddal.server.mysql.proto;

import java.util.ArrayList;

public class Com_Stmt_Prepare extends Packet {
    public String query="";
    
    public ArrayList<byte[]> getPayload() {
        ArrayList<byte[]> payload = new ArrayList<byte[]>();
        
        payload.add(Proto.build_byte(Flags.COM_STMT_PREPARE));
        payload.add(Proto.build_eop_str(this.query));
        
        return payload;
    }
    
    public static Com_Stmt_Prepare loadFromPacket(byte[] packet) {
        Com_Stmt_Prepare obj = new Com_Stmt_Prepare();
        Proto proto = new Proto(packet, 3);
        
        obj.sequenceId = proto.get_fixed_int(1);
        proto.get_filler(1);
        obj.query = proto.get_eop_str();

        return obj;
    }
}
