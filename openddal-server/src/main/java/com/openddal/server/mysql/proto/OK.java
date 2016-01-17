package com.openddal.server.mysql.proto;

import java.util.ArrayList;

public class OK extends Packet {
    public long affectedRows = 0;
    public long lastInsertId = 0;
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
        
        payload.add(Proto.build_byte(Flags.OK));
        payload.add(Proto.build_lenenc_int(this.affectedRows));
        payload.add(Proto.build_lenenc_int(this.lastInsertId));
        payload.add(Proto.build_fixed_int(2, this.statusFlags));
        payload.add(Proto.build_fixed_int(2, this.warnings));
        
        return payload;
    }
    
    public static OK loadFromPacket(byte[] packet) {
        OK obj = new OK();
        Proto proto = new Proto(packet, 3);
        
        obj.sequenceId = proto.get_fixed_int(1);
        proto.get_filler(1);
        obj.affectedRows = proto.get_lenenc_int();
        obj.lastInsertId = proto.get_lenenc_int();
        obj.statusFlags = proto.get_fixed_int(2);
        obj.warnings = proto.get_fixed_int(2);
        
        return obj;
    }
}
