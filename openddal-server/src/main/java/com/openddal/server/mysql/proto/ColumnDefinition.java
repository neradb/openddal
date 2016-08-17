package com.openddal.server.mysql.proto;

import java.util.ArrayList;

public class ColumnDefinition extends Packet {
    public String catalog;
    public String schema;
    public String table;
    public String org_table;
    public String name;
    public String org_name;
    public long characterSet;
    public long columnLength;
    public long flags;
    public long decimals;
    public long type = Flags.MYSQL_TYPE_VAR_STRING;
    
    public ColumnDefinition() {}
    
    public ColumnDefinition(String name) {
        // Set this up by default. Allow overrides if needed
        this.characterSet = Resultset.characterSet;
        this.name = name;
    }
    
    public ArrayList<byte[]> getPayload() {
        ArrayList<byte[]> payload = new ArrayList<byte[]>();
        
        payload.add(Proto.build_lenenc_str(this.catalog));
        payload.add(Proto.build_lenenc_str(this.schema));
        payload.add(Proto.build_lenenc_str(this.table));
        payload.add(Proto.build_lenenc_str(this.org_table));
        payload.add(Proto.build_lenenc_str(this.name));
        payload.add(Proto.build_lenenc_str(this.org_name));
        payload.add(Proto.build_filler(1, (byte)0x0c));
        payload.add(Proto.build_fixed_int(2, this.characterSet));
        payload.add(Proto.build_fixed_int(4, this.columnLength));
        payload.add(Proto.build_fixed_int(1, this.type));
        payload.add(Proto.build_fixed_int(2, this.flags));
        payload.add(Proto.build_fixed_int(1, this.decimals));
        payload.add(Proto.build_filler(2));
        
        return payload;
    }
    
    public static ColumnDefinition loadFromPacket(byte[] packet) {
        ColumnDefinition obj = new ColumnDefinition();
        Proto proto = new Proto(packet, 3);
        
        obj.sequenceId = proto.get_fixed_int(1);
        obj.catalog = proto.get_lenenc_str();
        obj.schema = proto.get_lenenc_str();
        obj.table = proto.get_lenenc_str();
        obj.org_table = proto.get_lenenc_str();
        obj.name = proto.get_lenenc_str();
        obj.org_name = proto.get_lenenc_str();
        proto.get_filler(1);
        obj.characterSet = proto.get_fixed_int(2);
        obj.columnLength = proto.get_fixed_int(4);
        obj.type = proto.get_fixed_int(1);
        obj.flags = proto.get_fixed_int(2);
        obj.decimals = proto.get_fixed_int(1);
        proto.get_filler(2);
        
        return obj;
    }
}
