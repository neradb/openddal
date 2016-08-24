package com.openddal.server.mysql.proto;

import java.util.ArrayList;

public class HandshakeResponse extends Packet {
    public long capabilityFlags = Flags.CLIENT_PROTOCOL_41;
    public long maxPacketSize = 0;
    public long characterSet = 0;
    public String username = "";
    public long authResponseLen = 0;
    public String authResponse = "";
    public String schema = "";
    public String pluginName = "";
    public long clientAttributesLen = 0;
    public String clientAttributes = "";
    
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
        
        if ((this.capabilityFlags & Flags.CLIENT_PROTOCOL_41) != 0) {
            payload.add( Proto.build_fixed_int(4, this.capabilityFlags));
            payload.add( Proto.build_fixed_int(4, this.maxPacketSize));
            payload.add( Proto.build_fixed_int(1, this.characterSet));
            payload.add( Proto.build_filler(23));
            payload.add( Proto.build_null_str(this.username));
            if (this.hasCapabilityFlag(Flags.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA)) {
                payload.add( Proto.build_lenenc_int(this.authResponseLen));
                payload.add( Proto.build_fixed_str(this.authResponseLen, this.authResponse));
            }
            else {
                if (this.hasCapabilityFlag(Flags.CLIENT_SECURE_CONNECTION)) {
                    payload.add( Proto.build_fixed_int(1, this.authResponseLen));
                    payload.add( Proto.build_fixed_str(this.authResponseLen, this.authResponse));
                }
                else
                    payload.add( Proto.build_null_str(this.authResponse));
            }
                
            if (this.hasCapabilityFlag(Flags.CLIENT_CONNECT_WITH_DB))
                payload.add( Proto.build_null_str(this.schema));
            
            if (this.hasCapabilityFlag(Flags.CLIENT_PLUGIN_AUTH))
                payload.add( Proto.build_null_str(this.pluginName));
                
            if (this.hasCapabilityFlag(Flags.CLIENT_CONNECT_ATTRS)) {
                payload.add( Proto.build_lenenc_int(this.clientAttributesLen));
                payload.add( Proto.build_eop_str(this.clientAttributes));
            }
        }
        else {
            payload.add( Proto.build_fixed_int(2, this.capabilityFlags));
            payload.add( Proto.build_fixed_int(3, this.maxPacketSize));
            payload.add( Proto.build_null_str(this.username));
            
            if (this.hasCapabilityFlag(Flags.CLIENT_CONNECT_WITH_DB)) {
                payload.add( Proto.build_null_str(this.authResponse));   
                payload.add( Proto.build_null_str(this.schema));
            }
            else
                payload.add( Proto.build_eop_str(this.authResponse));
            
        }
        
        return payload;
    }
    
    public static HandshakeResponse loadFromPacket(byte[] packet) {
        HandshakeResponse obj = new HandshakeResponse();
        Proto proto = new Proto(packet, 3);

        obj.sequenceId = proto.get_fixed_int(1);
        obj.capabilityFlags = proto.get_fixed_int(2);
        proto.offset -= 2;

        if (obj.hasCapabilityFlag(Flags.CLIENT_PROTOCOL_41)) {
            obj.capabilityFlags = proto.get_fixed_int(4);
            obj.maxPacketSize = proto.get_fixed_int(4);
            obj.characterSet = proto.get_fixed_int(1);
            proto.get_filler(23);
            obj.username = proto.get_null_str();

            if (obj.hasCapabilityFlag(Flags.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA)) {
                obj.authResponseLen = proto.get_lenenc_int();
                obj.authResponse = proto.get_fixed_str(obj.authResponseLen);
            } else {
                if (obj.hasCapabilityFlag(Flags.CLIENT_SECURE_CONNECTION)) {
                    obj.authResponseLen = proto.get_fixed_int(1);
                    obj.authResponse = proto.get_fixed_str(obj.authResponseLen);
                } else {
                    obj.authResponse = proto.get_null_str();
                }
            }

            if (obj.hasCapabilityFlag(Flags.CLIENT_CONNECT_WITH_DB))
                obj.schema = proto.get_null_str();

            if (obj.hasCapabilityFlag(Flags.CLIENT_PLUGIN_AUTH))
                obj.pluginName = proto.get_null_str();

            if (obj.hasCapabilityFlag(Flags.CLIENT_CONNECT_ATTRS)) {
                obj.clientAttributesLen = proto.get_lenenc_int();
                obj.clientAttributes = proto.get_eop_str();
            }
        } else {
            obj.capabilityFlags = proto.get_fixed_int(2);
            obj.maxPacketSize = proto.get_fixed_int(3);
            obj.username = proto.get_null_str();

            if (obj.hasCapabilityFlag(Flags.CLIENT_CONNECT_WITH_DB)) {
                obj.authResponse = proto.get_null_str();
                obj.schema = proto.get_null_str();
            } else
                obj.authResponse = proto.get_eop_str();
        }

        return obj;
    }

    @Override
    public String toString() {
        return "Authenticate[user=" + username + "]";
    }
    
    
}
