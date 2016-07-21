package com.openddal.server.mysql.proto;

import java.util.ArrayList;

public class AuthSwitchResponse extends Packet {
    public String authPluginResponse = "";

    public ArrayList<byte[]> getPayload() {
        ArrayList<byte[]> payload = new ArrayList<byte[]>();

        payload.add(Proto.build_eop_str(this.authPluginResponse));

        return payload;
    }

    public static AuthSwitchResponse loadFromPacket(byte[] packet) {
        AuthSwitchResponse obj = new AuthSwitchResponse();
        Proto proto = new Proto(packet, 3);

        obj.sequenceId = proto.get_fixed_int(1);
        obj.authPluginResponse = proto.get_eop_str();

        return obj;
    }
}
