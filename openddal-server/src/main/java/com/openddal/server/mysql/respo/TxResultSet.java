package com.openddal.server.mysql.respo;

import java.sql.ResultSet;
import java.sql.Types;

import com.openddal.result.SimpleResultSet;

public class TxResultSet {
    
    public static ResultSet getReadonlyResultSet(boolean readOnly) {
        SimpleResultSet result = new SimpleResultSet();
        result.addColumn("@@SESSION.TX_READ_ONLY", Types.INTEGER, Integer.MAX_VALUE, 0);
        result.addRow(readOnly ? 1 : 0);
        return result;
    }
    
    public static ResultSet getIsolationResultSet(int isolation) {
        SimpleResultSet result = new SimpleResultSet();
        result.addColumn("@@SESSION.TX_ISOLATION", Types.INTEGER, Integer.MAX_VALUE, 0);
        result.addRow(isolation);
        return result;
    }

}
