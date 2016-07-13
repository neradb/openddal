// jTDS JDBC Driver for Microsoft SQL Server and Sybase
// Copyright (C) 2004 The jTDS Project
//
// This library is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public
// License as published by the Free Software Foundation; either
// version 2.1 of the License, or (at your option) any later version.
//
// This library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public
// License along with this library; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
//
package com.openddal.test.jdbc;

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.openddal.test.BaseTestCase;

import junit.framework.Assert;

/**
 * Test JDBC3 extensions to <code>DatabaseMetaData</code>.
 *
 * @version $Id: DatabaseMetaDataJDBC3Test.java,v 1.2 2005-01-05 12:24:14 alin_sinpalean Exp $
 */
public class DatabaseMetaDataJDBC3Test extends BaseTestCase {
    
    private Connection con;

    public void setUp() throws Exception {
        con = getConnection();
    }

    public void tearDown() throws Exception {
        con.close();
    }

    /**
     * Test meta data functions that return boolean values.
     */
    public void testBooleanOptions() throws Exception {
        DatabaseMetaData dbmd = con.getMetaData();
        //
        // Test JDBC 3 items
        //
        assertTrue("locatorsUpdateCopy",dbmd.locatorsUpdateCopy());
        assertTrue("supportsGetGeneratedKeys", dbmd.supportsGetGeneratedKeys());
        assertTrue("supportsMultipleOpenResults", dbmd.supportsMultipleOpenResults());
        assertTrue("supportsNamedParameters", dbmd.supportsNamedParameters());
        assertFalse("supportsResultSetHoldability", dbmd.supportsResultSetHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT));
        assertFalse("supportsResultSetHoldability", dbmd.supportsResultSetHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT));
        assertTrue("supportsSavepoints", dbmd.supportsSavepoints());
        assertTrue("supportsStatementPooling", dbmd.supportsStatementPooling());
    }

    /**
     * Test meta data function that return integer values.
     */
    public void testIntOptions() throws Exception {
        DatabaseMetaData dbmd = con.getMetaData();
        //
        // JDBC3 functions
        //
        assertTrue("getDatabaseMajorVersion", dbmd.getDatabaseMajorVersion() >= 0);
        assertTrue("getDatabaseMinorVersion", dbmd.getDatabaseMinorVersion() >= 0);
        Assert.assertEquals("getResultSetHoldability",ResultSet.HOLD_CURSORS_OVER_COMMIT, dbmd.getResultSetHoldability());
        Assert.assertEquals("getSQLStateType",1, dbmd.getSQLStateType());
        Assert.assertEquals("getJDBCMajorVersion", 3, dbmd.getJDBCMajorVersion());
        Assert.assertEquals("getJDBCMinorVersion", 0, dbmd.getJDBCMinorVersion());
    }

    /**
     * Test meta data functions that return result sets.
     */
    public void testResultSets() throws Exception
    {
        DatabaseMetaData dbmd = con.getMetaData();
        ResultSet rs;
        //
        // JDBC3 Methods
        //
        rs = dbmd.getAttributes(null, null, null, null);
        assertTrue(checkColumnNames(rs, new String[]{"TYPE_CAT", "TYPE_SCHEM","TYPE_NAME","ATTR_NAME",
                "DATA_TYPE","ATTR_TYPE_NAME","ATTR_SIZE","DECIMAL_DIGITS","NUM_PREC_RADIX","NULLABLE",
                "REMARKS","ATTR_DEF","SQL_DATA_TYPE","SQL_DATETIME_SUB","CHAR_OCTET_LENGTH",
                "ORDINAL_POSITION","IS_NULLABLE","SCOPE_CATALOG","SCOPE_SCHEMA","SCOPE_TABLE","SOURCE_DATA_TYPE"}));
        assertFalse(rs.next());
        rs.close();
        //
        rs = dbmd.getSuperTables(null, null, "%");
        assertTrue(checkColumnNames(rs, new String[]{"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME","SUPERTABLE_NAME"}));
        assertFalse(rs.next());
        rs.close();
        //
        rs = dbmd.getSuperTypes(null, null, "%");
        assertTrue(checkColumnNames(rs, new String[]{"TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME",
                "SUPERTYPE_CAT", "SUPERTYPE_SCHEM", "SUPERTYPE_NAME"}));
        assertFalse(rs.next());
        rs.close();
    }
    
    /**
     * Utility method to check column names and number.
     *
     * @param rs    the result set to check
     * @param names the list of column names to compare to result set
     * @return the <code>boolean</code> value true if the columns match
     */
    protected boolean checkColumnNames(ResultSet rs, String[] names)
            throws SQLException {
        ResultSetMetaData rsmd = rs.getMetaData();
        if (rsmd.getColumnCount() < names.length) {
            System.out.println("Cols=" + rsmd.getColumnCount());
            return false;
        }

        for (int i = 1; i <= names.length; i++) {
            if (names[i - 1].length() > 0
                    && !rsmd.getColumnLabel(i).equals(names[i - 1])) {
                System.out.println(
                        names[i - 1] + " = " + rsmd.getColumnLabel(i));
                return false;
            }
        }

        return true;
    }

}
