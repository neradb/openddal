package com.openddal.server.mysql.proto;

import java.util.ArrayList;

public class Com_Stmt_Prepare_Ok extends Packet {
    public String query="";
    
    public ArrayList<byte[]> getPayload() {
        ArrayList<byte[]> payload = new ArrayList<byte[]>();
        
        return payload;
    }
    
    public static Com_Stmt_Prepare_Ok loadFromPacket(byte[] packet) {
        Com_Stmt_Prepare_Ok obj = new Com_Stmt_Prepare_Ok();
        Proto proto = new Proto(packet, 3);
        
        obj.sequenceId = proto.get_fixed_int(1);

        return obj;
    }
}
