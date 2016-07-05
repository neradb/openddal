
package com.openddal.server.mysql.respo;

import java.sql.ResultSet;
import java.sql.Types;

import com.openddal.result.SimpleResultSet;
import com.openddal.server.mysql.MySQLServer;


public final class ShowVersion {

    public static ResultSet getResultSet() {
        SimpleResultSet result = new SimpleResultSet();
        result.addColumn("VERSION", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addRow(MySQLServer.SERVER_VERSION);
        return result;
    }

    public static ResultSet getCommentResultSet() {
        SimpleResultSet result = new SimpleResultSet();
        result.addColumn("@@VERSION_COMMENT", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addRow("OpenDDAL MySQL Protocol Server");
        return result;
    }
}