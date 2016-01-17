package com.openddal.server.mysql.proto;

import java.util.ArrayList;

public class SSLRequest extends Packet {
    public long capabilityFlags = Flags.CLIENT_PROTOCOL_41;
    public long maxPacketSize = 0;
    public long characterSet = 0;

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

        payload.add( Proto.build_fixed_int(4, this.capabilityFlags));
        payload.add( Proto.build_fixed_int(4, this.maxPacketSize));
        payload.add( Proto.build_fixed_int(1, this.characterSet));
        payload.add( Proto.build_filler(23));

        return payload;
    }

    public static HandshakeResponse loadFromPacket(byte[] packet) {
        HandshakeResponse obj = new HandshakeResponse();
        Proto proto = new Proto(packet, 3);

        obj.capabilityFlags = proto.get_fixed_int(4);
        obj.maxPacketSize = proto.get_fixed_int(4);
        obj.characterSet = proto.get_fixed_int(1);
        proto.get_filler(23);

        return obj;
    }
}
