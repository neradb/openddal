package com.openddal.server.mysql.proto;

import java.util.ArrayList;

public class Handshake extends Packet {
    public long protocolVersion = 0x0a;
    public String serverVersion = "";
    public long connectionId = 0;
    public String challenge1 = "";
    public long capabilityFlags = Flags.CLIENT_PROTOCOL_41;
    public long characterSet = 0;
    public long statusFlags = 0;
    public String challenge2 = "";
    public long authPluginDataLength = 0;
    public String authPluginName = "";

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

        payload.add( Proto.build_fixed_int(1, this.protocolVersion));
        payload.add( Proto.build_null_str(this.serverVersion));
        payload.add( Proto.build_fixed_int(4, this.connectionId));
        payload.add( Proto.build_fixed_str(8, this.challenge1));
        payload.add( Proto.build_filler(1));
        payload.add(Proto.build_fixed_int(2, this.capabilityFlags & 0xffff));
        payload.add( Proto.build_fixed_int(1, this.characterSet));
        payload.add( Proto.build_fixed_int(2, this.statusFlags));
        payload.add(Proto.build_fixed_int(2, this.capabilityFlags >> 16));

        if (this.hasCapabilityFlag(Flags.CLIENT_SECURE_CONNECTION)) {
            payload.add( Proto.build_fixed_int(1, this.authPluginDataLength));
        }
        else {
            payload.add( Proto.build_filler(1));
        }

        payload.add( Proto.build_fixed_str(10, ""));

        if (this.hasCapabilityFlag(Flags.CLIENT_SECURE_CONNECTION)) {
            payload.add( Proto.build_fixed_str(Math.max(13, this.authPluginDataLength - 8), this.challenge2));
        }

        if (this.hasCapabilityFlag(Flags.CLIENT_PLUGIN_AUTH)) {
            payload.add( Proto.build_null_str(this.authPluginName));
        }

        return payload;
    }

    public static Handshake loadFromPacket(byte[] packet) {
        Handshake obj = new Handshake();
        Proto proto = new Proto(packet, 3);

        obj.sequenceId = proto.get_fixed_int(1);
        obj.protocolVersion = proto.get_fixed_int(1);
        obj.serverVersion = proto.get_null_str();
        obj.connectionId = proto.get_fixed_int(4);
        obj.challenge1 = proto.get_fixed_str(8);
        proto.get_filler(1);
        obj.capabilityFlags = proto.get_fixed_int(2) & 0xffff;

        if (proto.has_remaining_data()) {
            obj.characterSet = proto.get_fixed_int(1);
            obj.statusFlags = proto.get_fixed_int(2);
            obj.setCapabilityFlag(proto.get_fixed_int(2) << 16);

            if (obj.hasCapabilityFlag(Flags.CLIENT_PLUGIN_AUTH)) {
                obj.authPluginDataLength = proto.get_fixed_int(1);
            }
            else {
                proto.get_filler(1);
            }

            proto.get_filler(10);

            if (obj.hasCapabilityFlag(Flags.CLIENT_SECURE_CONNECTION)) {
                obj.challenge2 = proto.get_fixed_str(Math.max(13, obj.authPluginDataLength - 8));
            }

            if (obj.hasCapabilityFlag(Flags.CLIENT_PLUGIN_AUTH)) {
                obj.authPluginName = proto.get_null_str();
            }
        }

        return obj;
    }
}
