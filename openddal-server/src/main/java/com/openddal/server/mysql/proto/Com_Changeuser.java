package com.openddal.server.mysql.proto;

import java.util.ArrayList;

public class Com_Changeuser extends Packet {
    public String user = "";
    public String authResponse = "";
    public String schema = "";
    public long characterSet = 0;
    public long capabilityFlags = 0;
    
    public void setCapabilityFlag(long flag) {
        this.capabilityFlags |= flag;
    }
    
    public void removeCapabilityFlag(long flag) {
        this.capabilityFlags &= ~flag;
    }
    
    public void toggleCapabilityFlag(long flag) {
        this.capabilityFlags ^= flag;
    }
    
    public boolean hasCapabilityFlag(long flag) {
        return ((this.capabilityFlags & flag) == flag);
    }
    
    public ArrayList<byte[]> getPayload() {
        ArrayList<byte[]> payload = new ArrayList<byte[]>();
        
        payload.add(Proto.build_byte(Flags.COM_CHANGE_USER));
        payload.add(Proto.build_null_str(this.user));
        if (!this.hasCapabilityFlag(Flags.CLIENT_SECURE_CONNECTION))
            payload.add(Proto.build_lenenc_str(this.authResponse));
        else
            payload.add(Proto.build_null_str(this.authResponse));
        payload.add(Proto.build_null_str(this.schema));
        payload.add(Proto.build_fixed_int(2, this.characterSet));
        
        return payload;
    }
    
    public static Com_Changeuser loadFromPacket(byte[] packet) {
        Com_Changeuser obj = new Com_Changeuser();
        Proto proto = new Proto(packet, 3);
        
        obj.sequenceId = proto.get_fixed_int(1);
        proto.get_filler(1);
        obj.user = proto.get_null_str();
        if (!obj.hasCapabilityFlag(Flags.CLIENT_SECURE_CONNECTION))
            obj.authResponse = proto.get_lenenc_str();
        else
            obj.authResponse = proto.get_null_str();
        obj.schema = proto.get_null_str();
        obj.characterSet = proto.get_fixed_int(2);
        
        return obj;
    }
}
