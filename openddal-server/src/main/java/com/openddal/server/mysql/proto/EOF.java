package com.openddal.server.mysql.proto;

import java.util.ArrayList;

public class EOF extends Packet {
    public long statusFlags = 0;
    public long warnings = 0;
    
    public void setStatusFlag(long flag) {
        this.statusFlags |= flag;
    }
    
    public void removeStatusFlag(long flag) {
        this.statusFlags &= ~flag;
    }
    
    public void toggleStatusFlag(long flag) {
        this.statusFlags ^= flag;
    }
    
    public boolean hasStatusFlag(long flag) {
        return ((this.statusFlags & flag) == flag);
    }
    
    public ArrayList<byte[]> getPayload() {
        ArrayList<byte[]> payload = new ArrayList<byte[]>();
        
        payload.add(Proto.build_byte(Flags.EOF));
        payload.add(Proto.build_fixed_int(2, this.warnings));
        payload.add(Proto.build_fixed_int(2, this.statusFlags));
        
        return payload;
    }
    
    public static EOF loadFromPacket(byte[] packet) {
        EOF obj = new EOF();
        Proto proto = new Proto(packet, 3);
        
        obj.sequenceId = proto.get_fixed_int(1);
        proto.get_filler(1);
        obj.warnings = proto.get_fixed_int(2);
        obj.statusFlags = proto.get_fixed_int(2);
        
        return obj;
    }
}
