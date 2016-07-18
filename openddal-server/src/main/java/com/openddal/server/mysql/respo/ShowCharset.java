package com.openddal.server.mysql.respo;

import java.sql.ResultSet;
import java.sql.Types;

import com.openddal.result.SimpleResultSet;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public final class ShowCharset {



    public static ResultSet getResultSet() {
        SimpleResultSet result = new SimpleResultSet();
        result.addColumn("Charset", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Description", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Default collation", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Maxlen", Types.VARCHAR, Integer.MAX_VALUE, 0);

        result.addRow("utf8", "UTF-8 Unicode", "utf8_general_ci", "3");
        return result;
    }

    public static ResultSet getCollationResultSet() {
        SimpleResultSet result = new SimpleResultSet();
        result.addColumn("Collation", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Charset", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Id", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Default", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Compiled", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Sortlen", Types.VARCHAR, Integer.MAX_VALUE, 0);

        result.addRow("utf8_general_ci", "utf8", "33", "Yes", "Yes", "1");
        return result;
    }


}