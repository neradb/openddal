package com.openddal.server.mysql.proto;

import java.util.ArrayList;

public class ResultSetPacket {
    public long sequenceId = 1;
    public static long characterSet = 0;
    
    public ArrayList<ColumnPacket> columns = new ArrayList<ColumnPacket>();
    public ArrayList<RowPacket> rows = new ArrayList<RowPacket>();
    
    public ArrayList<byte[]> toPackets() {
        ArrayList<byte[]> packets = new ArrayList<byte[]>();
        
        ColCountPacket colCount = new ColCountPacket();
        colCount.sequenceId = this.sequenceId;
        this.sequenceId++;
        colCount.colCount = this.columns.size();
        packets.add(colCount.toPacket());
        
        for (ColumnPacket col: this.columns) {
            col.sequenceId = this.sequenceId;
            this.sequenceId++;
            packets.add(col.toPacket());
        }
        
        packets.add(eofPacket());
        
        for (RowPacket row: this.rows) {
            row.sequenceId = this.sequenceId;
            this.sequenceId++;
            packets.add(row.toPacket());
        }
        
        packets.add(eofPacket());
        
        return packets;
    }
    
    public ArrayList<byte[]> toHeadPackets() {
        ArrayList<byte[]> packets = new ArrayList<byte[]>();
        ColCountPacket colCount = new ColCountPacket();
        colCount.sequenceId = this.sequenceId;
        this.sequenceId++;
        colCount.colCount = this.columns.size();
        packets.add(colCount.toPacket());

        for (ColumnPacket col : this.columns) {
            col.sequenceId = this.sequenceId;
            this.sequenceId++;
            packets.add(col.toPacket());
        }
        packets.add(eofPacket());
        return packets;
    }

    public ArrayList<byte[]> toRowPackets() {
        ArrayList<byte[]> packets = new ArrayList<byte[]>();
        for (RowPacket row : this.rows) {
            row.sequenceId = this.sequenceId;
            this.sequenceId++;
            packets.add(row.toPacket());
        }
        packets.add(eofPacket());
        return packets;
    }

    public byte[] eofPacket() {
        EOF eof = new EOF();
        eof.sequenceId = this.sequenceId;
        this.sequenceId++;
        return eof.toPacket();
    }

    public void addColumn(ColumnPacket column) {
        this.columns.add(column);
    }
    
    public void addRow(RowPacket row) {
        this.rows.add(row);
    }
}
