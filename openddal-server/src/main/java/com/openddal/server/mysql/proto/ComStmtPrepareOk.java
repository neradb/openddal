package com.openddal.server.mysql.proto;

import java.util.ArrayList;

public class ComStmtPrepareOk extends Packet {
    public String query="";
    
    public ArrayList<byte[]> getPayload() {
        ArrayList<byte[]> payload = new ArrayList<byte[]>();
        
        return payload;
    }
    
    public static ComStmtPrepareOk loadFromPacket(byte[] packet) {
        ComStmtPrepareOk obj = new ComStmtPrepareOk();
        Proto proto = new Proto(packet, 3);
        
        obj.sequenceId = proto.get_fixed_int(1);

        return obj;
    }
}
