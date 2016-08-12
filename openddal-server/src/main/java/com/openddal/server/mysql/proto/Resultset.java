package com.openddal.server.mysql.proto;

import java.util.ArrayList;

public class Resultset {
    public long sequenceId = 1;
    public static long characterSet = 0;
    
    public ArrayList<ColumnDefinition> columns = new ArrayList<ColumnDefinition>();
    public ArrayList<ResultsetRow> rows = new ArrayList<ResultsetRow>();
    
    public ArrayList<byte[]> toPackets() {
        ArrayList<byte[]> packets = new ArrayList<byte[]>();
        
        ColCountPacket colCount = new ColCountPacket();
        colCount.sequenceId = this.sequenceId;
        this.sequenceId++;
        colCount.colCount = this.columns.size();
        packets.add(colCount.toPacket());
        
        for (ColumnDefinition col: this.columns) {
            col.sequenceId = this.sequenceId;
            this.sequenceId++;
            packets.add(col.toPacket());
        }
        
        packets.add(eofPacket());
        
        for (ResultsetRow row: this.rows) {
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

        for (ColumnDefinition col : this.columns) {
            col.sequenceId = this.sequenceId;
            this.sequenceId++;
            packets.add(col.toPacket());
        }
        packets.add(eofPacket());
        return packets;
    }

    public ArrayList<byte[]> toRowPackets() {
        ArrayList<byte[]> packets = new ArrayList<byte[]>();
        for (ResultsetRow row : this.rows) {
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

    public void addColumn(ColumnDefinition column) {
        this.columns.add(column);
    }
    
    public void addRow(ResultsetRow row) {
        this.rows.add(row);
    }
}
