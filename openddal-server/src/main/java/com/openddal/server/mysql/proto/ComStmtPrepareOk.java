package com.openddal.server.mysql.proto;

import java.util.ArrayList;

public class ComStmtPrepareOk extends Packet {
    
    public byte flag;
    public long statementId;
    public int columnsNumber;
    public int parametersNumber;
    public byte filler;
    public int warningCount;
    
    public ArrayList<byte[]> getPayload() {
        ArrayList<byte[]> payload = new ArrayList<byte[]>();
        payload.add( Proto.build_fixed_int(1, this.flag));
        payload.add( Proto.build_fixed_int(4, this.statementId));
        payload.add( Proto.build_fixed_int(2, this.columnsNumber));
        payload.add( Proto.build_fixed_int(2, this.parametersNumber));
        payload.add( Proto.build_filler(1));
        payload.add( Proto.build_fixed_int(2, this.warningCount));
        return payload;
    }
    
    public static ComStmtPrepareOk loadFromPacket(byte[] packet) {
        ComStmtPrepareOk obj = new ComStmtPrepareOk();
        Proto proto = new Proto(packet, 3);
        obj.sequenceId = proto.get_fixed_int(1);

        return obj;
    }
}
