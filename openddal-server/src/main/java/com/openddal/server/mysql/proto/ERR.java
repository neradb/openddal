package com.openddal.server.mysql.proto;

/*
 * A MySQL ERR Packet
 *
 * https://dev.mysql.com/doc/refman/5.5/en/error-messages-server.html
 * https://dev.mysql.com/doc/refman/5.5/en/error-messages-client.html
 *
 */

import java.util.ArrayList;

public class ERR extends Packet {
    public long errorCode = 0;
    public String sqlState = "HY000";
    public String errorMessage = "";
    
    public ArrayList<byte[]> getPayload() {
        ArrayList<byte[]> payload = new ArrayList<byte[]>();
        
        payload.add(Proto.build_byte(Flags.ERR));
        payload.add(Proto.build_fixed_int(2, this.errorCode));
        payload.add(Proto.build_byte((byte)'#'));
        payload.add(Proto.build_fixed_str(5, this.sqlState));
        payload.add(Proto.build_eop_str(this.errorMessage));
        
        return payload;
    }
    
    public static ERR loadFromPacket(byte[] packet) {
        ERR obj = new ERR();
        Proto proto = new Proto(packet, 3);
        
        obj.sequenceId = proto.get_fixed_int(1);
        proto.get_filler(1);
        obj.errorCode = proto.get_fixed_int(2);
        proto.get_filler(1);
        obj.sqlState = proto.get_fixed_str(5);
        obj.errorMessage = proto.get_eop_str();
        
        return obj;
    }
}
