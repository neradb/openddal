/*
 * Copyright 2014-2016 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the “License”);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an “AS IS” BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.openddal.test.jdbc;

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

import org.junit.Test;

import com.openddal.message.ErrorCode;
import com.openddal.test.BaseTestCase;

import junit.framework.Assert;

/**
 * Updatable result set tests.
 */
public class UpdatableResultSetTestCase extends BaseTestCase {


    @Test
    public void test() throws Exception {
        testDetectUpdatable();
        testUpdateLob();
        testScroll();
        testUpdateDeleteInsert();
        testUpdateDataType();
        testUpdateResetRead();
    }

    private void testDetectUpdatable() throws SQLException {
        Connection conn = getConnection();
        Statement stat;
        ResultSet rs;
        stat = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_UPDATABLE);

        stat.execute("create table test(id int primary key, name varchar)");
        rs = stat.executeQuery("select * from test");
        Assert.assertEquals(ResultSet.CONCUR_UPDATABLE, rs.getConcurrency());
        rs = stat.executeQuery("select name from test");
        Assert.assertEquals(ResultSet.CONCUR_READ_ONLY, rs.getConcurrency());
        stat.execute("drop table test");

        stat.execute("create table test(a int, b int, " +
                "name varchar, primary key(a, b))");
        rs = stat.executeQuery("select * from test");
        Assert.assertEquals(ResultSet.CONCUR_UPDATABLE, rs.getConcurrency());
        rs = stat.executeQuery("select a, name from test");
        Assert.assertEquals(ResultSet.CONCUR_READ_ONLY, rs.getConcurrency());
        rs = stat.executeQuery("select b, name from test");
        Assert.assertEquals(ResultSet.CONCUR_READ_ONLY, rs.getConcurrency());
        rs = stat.executeQuery("select b, name, a from test");
        Assert.assertEquals(ResultSet.CONCUR_UPDATABLE, rs.getConcurrency());
        stat.execute("drop table test");

        stat.execute("create table test(a int, b int, name varchar)");
        stat.execute("create unique index on test(b, a)");
        rs = stat.executeQuery("select * from test");
        Assert.assertEquals(ResultSet.CONCUR_UPDATABLE, rs.getConcurrency());
        rs = stat.executeQuery("select a, name from test");
        Assert.assertEquals(ResultSet.CONCUR_READ_ONLY, rs.getConcurrency());
        rs = stat.executeQuery("select b, name from test");
        Assert.assertEquals(ResultSet.CONCUR_READ_ONLY, rs.getConcurrency());
        rs = stat.executeQuery("select b, name, a from test");
        Assert.assertEquals(ResultSet.CONCUR_UPDATABLE, rs.getConcurrency());
        stat.execute("drop table test");

        stat.execute("create table test(a int, b int, c int unique, " +
                "name varchar, primary key(a, b))");
        rs = stat.executeQuery("select * from test");
        Assert.assertEquals(ResultSet.CONCUR_UPDATABLE, rs.getConcurrency());
        rs = stat.executeQuery("select a, name, c from test");
        Assert.assertEquals(ResultSet.CONCUR_UPDATABLE, rs.getConcurrency());
        rs = stat.executeQuery("select b, a, name, c from test");
        Assert.assertEquals(ResultSet.CONCUR_UPDATABLE, rs.getConcurrency());
        stat.execute("drop table test");

        stat.execute("create table test(id int primary key, " +
                "a int, b int, i int, j int, k int, name varchar)");
        stat.execute("create unique index on test(b, a)");
        stat.execute("create unique index on test(i, j)");
        stat.execute("create unique index on test(a, j)");
        rs = stat.executeQuery("select * from test");
        Assert.assertEquals(ResultSet.CONCUR_UPDATABLE, rs.getConcurrency());
        rs = stat.executeQuery("select a, name, b from test");
        Assert.assertEquals(ResultSet.CONCUR_UPDATABLE, rs.getConcurrency());
        rs = stat.executeQuery("select a, name, b from test");
        Assert.assertEquals(ResultSet.CONCUR_UPDATABLE, rs.getConcurrency());
        rs = stat.executeQuery("select i, b, k, name from test");
        Assert.assertEquals(ResultSet.CONCUR_READ_ONLY, rs.getConcurrency());
        rs = stat.executeQuery("select a, i, name from test");
        Assert.assertEquals(ResultSet.CONCUR_READ_ONLY, rs.getConcurrency());
        rs = stat.executeQuery("select b, i, k, name from test");
        Assert.assertEquals(ResultSet.CONCUR_READ_ONLY, rs.getConcurrency());
        rs = stat.executeQuery("select a, k, j, name from test");
        Assert.assertEquals(ResultSet.CONCUR_UPDATABLE, rs.getConcurrency());
        stat.execute("drop table test");

        conn.close();
    }

    private void testUpdateLob() throws SQLException {
        Connection conn = getConnection();
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE object_index " +
                "(id integer primary key, object other, number integer)");

        PreparedStatement prep = conn.prepareStatement(
                "INSERT INTO object_index (id,object)  VALUES (1,?)");
        prep.setObject(1, "hello", Types.JAVA_OBJECT);
        prep.execute();

        ResultSet rs = stat.executeQuery(
                "SELECT object,id,number FROM object_index WHERE id =1");
        rs.next();
        Assert.assertEquals("hello", rs.getObject(1).toString());
        stat = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_UPDATABLE);
        rs = stat.executeQuery("SELECT object,id,number FROM object_index WHERE id =1");
        rs.next();
        Assert.assertEquals("hello", rs.getObject(1).toString());
        rs.updateInt(2, 1);
        rs.updateRow();
        rs.close();
        stat = conn.createStatement();
        rs = stat.executeQuery("SELECT object,id,number FROM object_index WHERE id =1");
        rs.next();
        Assert.assertEquals("hello", rs.getObject(1).toString());
        conn.close();
    }

    private void testUpdateResetRead() throws SQLException {
        Connection conn = getConnection();
        Statement stat = conn.createStatement(
                ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255))");
        stat.execute("INSERT INTO TEST VALUES(1, 'Hello')");
        stat.execute("INSERT INTO TEST VALUES(2, 'World')");
        ResultSet rs = stat.executeQuery("SELECT * FROM TEST ORDER BY ID");
        rs.next();
        rs.updateInt(1, 10);
        rs.updateRow();
        rs.next();

        rs.updateString(2, "Welt");
        rs.cancelRowUpdates();
        rs.updateString(2, "Welt");

        rs.updateRow();
        rs.beforeFirst();
        rs.next();
        Assert.assertEquals(10, rs.getInt(1));
        Assert.assertEquals("Hello", rs.getString(2));
        rs.next();
        Assert.assertEquals(2, rs.getInt(1));
        Assert.assertEquals("Welt", rs.getString(2));

        assertFalse(rs.isClosed());
        rs.close();
        assertTrue(rs.isClosed());

        conn.close();
    }

    private void testScroll() throws SQLException {
        Connection conn = getConnection();
        Statement stat = conn.createStatement(
                ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR)");
        stat.execute("INSERT INTO TEST VALUES(1, 'Hello'), (2, 'World'), (3, 'Test')");

        ResultSet rs = stat.executeQuery("SELECT * FROM TEST ORDER BY ID");

        assertTrue(rs.isBeforeFirst());
        assertFalse(rs.isAfterLast());
        Assert.assertEquals(0, rs.getRow());

        rs.next();
        assertFalse(rs.isBeforeFirst());
        assertFalse(rs.isAfterLast());
        Assert.assertEquals(1, rs.getInt(1));
        Assert.assertEquals(1, rs.getRow());

        rs.next();
        assertThrows(ErrorCode.RESULT_SET_READONLY, rs).insertRow();
        assertFalse(rs.isBeforeFirst());
        assertFalse(rs.isAfterLast());
        Assert.assertEquals(2, rs.getInt(1));
        Assert.assertEquals(2, rs.getRow());

        rs.next();
        assertFalse(rs.isBeforeFirst());
        assertFalse(rs.isAfterLast());
        Assert.assertEquals(3, rs.getInt(1));
        Assert.assertEquals(3, rs.getRow());

        assertFalse(rs.next());
        assertFalse(rs.isBeforeFirst());
        assertTrue(rs.isAfterLast());
        Assert.assertEquals(0, rs.getRow());

        assertTrue(rs.first());
        Assert.assertEquals(1, rs.getInt(1));
        Assert.assertEquals(1, rs.getRow());

        assertTrue(rs.last());
        Assert.assertEquals(3, rs.getInt(1));
        Assert.assertEquals(3, rs.getRow());

        assertTrue(rs.relative(0));
        Assert.assertEquals(3, rs.getRow());

        assertTrue(rs.relative(-1));
        Assert.assertEquals(2, rs.getRow());

        assertTrue(rs.relative(1));
        Assert.assertEquals(3, rs.getRow());

        assertFalse(rs.relative(100));
        assertTrue(rs.isAfterLast());

        assertFalse(rs.absolute(0));
        Assert.assertEquals(0, rs.getRow());

        assertTrue(rs.absolute(1));
        Assert.assertEquals(1, rs.getRow());

        assertTrue(rs.absolute(2));
        Assert.assertEquals(2, rs.getRow());

        assertTrue(rs.absolute(3));
        Assert.assertEquals(3, rs.getRow());

        assertFalse(rs.absolute(4));
        Assert.assertEquals(0, rs.getRow());

        // allowed for compatibility
        assertFalse(rs.absolute(0));

        assertTrue(rs.absolute(3));
        Assert.assertEquals(3, rs.getRow());

        assertTrue(rs.absolute(-1));
        Assert.assertEquals(3, rs.getRow());

        assertFalse(rs.absolute(4));
        assertTrue(rs.isAfterLast());

        assertFalse(rs.absolute(5));
        assertTrue(rs.isAfterLast());

        assertTrue(rs.previous());
        Assert.assertEquals(3, rs.getRow());

        assertTrue(rs.previous());
        Assert.assertEquals(2, rs.getRow());

        conn.close();
    }

    private void testUpdateDataType() throws Exception {
        Connection conn = getConnection();
        Statement stat = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_UPDATABLE);
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255), "
                + "DEC DECIMAL(10,2), BOO BIT, BYE TINYINT, BIN BINARY(100), "
                + "D DATE, T TIME, TS TIMESTAMP, DB DOUBLE, R REAL, L BIGINT, "
                + "O_I INT, SH SMALLINT, CL CLOB, BL BLOB)");
        ResultSet rs = stat.executeQuery("SELECT * FROM TEST");
        ResultSetMetaData meta = rs.getMetaData();
        Assert.assertEquals("java.lang.Integer", meta.getColumnClassName(1));
        Assert.assertEquals("java.lang.String", meta.getColumnClassName(2));
        Assert.assertEquals("java.math.BigDecimal", meta.getColumnClassName(3));
        Assert.assertEquals("java.lang.Boolean", meta.getColumnClassName(4));
        Assert.assertEquals("java.lang.Byte", meta.getColumnClassName(5));
        Assert.assertEquals("[B", meta.getColumnClassName(6));
        Assert.assertEquals("java.sql.Date", meta.getColumnClassName(7));
        Assert.assertEquals("java.sql.Time", meta.getColumnClassName(8));
        Assert.assertEquals("java.sql.Timestamp", meta.getColumnClassName(9));
        Assert.assertEquals("java.lang.Double", meta.getColumnClassName(10));
        Assert.assertEquals("java.lang.Float", meta.getColumnClassName(11));
        Assert.assertEquals("java.lang.Long", meta.getColumnClassName(12));
        Assert.assertEquals("java.lang.Integer", meta.getColumnClassName(13));
        Assert.assertEquals("java.lang.Short", meta.getColumnClassName(14));
        Assert.assertEquals("java.sql.Clob", meta.getColumnClassName(15));
        Assert.assertEquals("java.sql.Blob", meta.getColumnClassName(16));
        rs.moveToInsertRow();
        rs.updateInt(1, 0);
        rs.updateNull(2);
        rs.updateNull("DEC");
        // 'not set' values are set to null
        assertThrows(ErrorCode.NO_DATA_AVAILABLE, rs).cancelRowUpdates();
        rs.insertRow();

        rs.moveToInsertRow();
        rs.updateInt(1, 1);
        rs.updateString(2, null);
        rs.updateBigDecimal(3, null);
        rs.updateBoolean(4, false);
        rs.updateByte(5, (byte) 0);
        rs.updateBytes(6, null);
        rs.updateDate(7, null);
        rs.updateTime(8, null);
        rs.updateTimestamp(9, null);
        rs.updateDouble(10, 0.0);
        rs.updateFloat(11, (float) 0.0);
        rs.updateLong(12, 0L);
        rs.updateObject(13, null);
        rs.updateShort(14, (short) 0);
        rs.updateCharacterStream(15, new StringReader("test"), 0);
        rs.updateBinaryStream(16,
                new ByteArrayInputStream(new byte[]{(byte) 0xff, 0x00}), 0);
        rs.insertRow();

        rs.moveToInsertRow();
        rs.updateInt("ID", 2);
        rs.updateString("NAME", "+");
        rs.updateBigDecimal("DEC", new BigDecimal("1.2"));
        rs.updateBoolean("BOO", true);
        rs.updateByte("BYE", (byte) 0xff);
        rs.updateBytes("BIN", new byte[]{0x00, (byte) 0xff});
        rs.updateDate("D", Date.valueOf("2005-09-21"));
        rs.updateTime("T", Time.valueOf("21:46:28"));
        rs.updateTimestamp("TS",
                Timestamp.valueOf("2005-09-21 21:47:09.567890123"));
        rs.updateDouble("DB", 1.725);
        rs.updateFloat("R", (float) 2.5);
        rs.updateLong("L", Long.MAX_VALUE);
        rs.updateObject("O_I", 10);
        rs.updateShort("SH", Short.MIN_VALUE);
        // auml, ouml, uuml
        rs.updateCharacterStream("CL", new StringReader("\u00ef\u00f6\u00fc"), 0);
        rs.updateBinaryStream("BL",
                new ByteArrayInputStream(new byte[]{(byte) 0xab, 0x12}), 0);
        rs.insertRow();

        rs.moveToInsertRow();
        rs.updateInt("ID", 3);
        rs.updateCharacterStream("CL", new StringReader("\u00ef\u00f6\u00fc"));
        rs.updateBinaryStream("BL",
                new ByteArrayInputStream(new byte[]{(byte) 0xab, 0x12}));
        rs.insertRow();

        rs.moveToInsertRow();
        rs.updateInt("ID", 4);
        rs.updateCharacterStream(15, new StringReader("\u00ef\u00f6\u00fc"));
        rs.updateBinaryStream(16,
                new ByteArrayInputStream(new byte[]{(byte) 0xab, 0x12}));
        rs.insertRow();

        rs.moveToInsertRow();
        rs.updateInt("ID", 5);
        rs.updateClob("CL", new StringReader("\u00ef\u00f6\u00fc"));
        rs.updateBlob("BL",
                new ByteArrayInputStream(new byte[]{(byte) 0xab, 0x12}));
        rs.insertRow();

        rs.moveToInsertRow();
        rs.updateInt("ID", 6);
        rs.updateClob(15, new StringReader("\u00ef\u00f6\u00fc"));
        rs.updateBlob(16,
                new ByteArrayInputStream(new byte[]{(byte) 0xab, 0x12}));
        rs.insertRow();

        rs.moveToInsertRow();
        rs.updateInt("ID", 7);
        rs.updateNClob("CL", new StringReader("\u00ef\u00f6\u00fc"));
        Blob b = conn.createBlob();
        OutputStream out = b.setBinaryStream(1);
        out.write(new byte[]{(byte) 0xab, 0x12});
        out.close();
        rs.updateBlob("BL", b);
        rs.insertRow();

        rs.moveToInsertRow();
        rs.updateInt("ID", 8);
        rs.updateNClob(15, new StringReader("\u00ef\u00f6\u00fc"));
        rs.updateBlob(16, b);
        rs.insertRow();

        rs.moveToInsertRow();
        rs.updateInt("ID", 9);
        rs.updateNClob("CL", new StringReader("\u00ef\u00f6\u00fc"), -1);
        rs.updateBlob("BL", b);
        rs.insertRow();

        rs.moveToInsertRow();
        rs.updateInt("ID", 10);
        rs.updateNClob(15, new StringReader("\u00ef\u00f6\u00fc"), -1);
        rs.updateBlob(16, b);
        rs.insertRow();

        rs.moveToInsertRow();
        rs.updateInt("ID", 11);
        rs.updateNCharacterStream("CL",
                new StringReader("\u00ef\u00f6\u00fc"), -1);
        rs.updateBlob("BL", b);
        rs.insertRow();

        rs.moveToInsertRow();
        rs.updateInt("ID", 12);
        rs.updateNCharacterStream(15,
                new StringReader("\u00ef\u00f6\u00fc"), -1);
        rs.updateBlob(16, b);
        rs.insertRow();

        rs.moveToInsertRow();
        rs.updateInt("ID", 13);
        rs.updateNCharacterStream("CL",
                new StringReader("\u00ef\u00f6\u00fc"));
        rs.updateBlob("BL", b);
        rs.insertRow();

        rs.moveToInsertRow();
        rs.updateInt("ID", 14);
        rs.updateNCharacterStream(15,
                new StringReader("\u00ef\u00f6\u00fc"));
        rs.updateBlob(16, b);
        rs.insertRow();

        rs = stat.executeQuery("SELECT * FROM TEST ORDER BY ID NULLS FIRST");
        rs.next();
        assertTrue(rs.getInt(1) == 0);
        assertTrue(rs.getString(2) == null && rs.wasNull());
        assertTrue(rs.getBigDecimal(3) == null && rs.wasNull());
        assertTrue(!rs.getBoolean(4) && rs.wasNull());
        assertTrue(rs.getByte(5) == 0 && rs.wasNull());
        assertTrue(rs.getBytes(6) == null && rs.wasNull());
        assertTrue(rs.getDate(7) == null && rs.wasNull());
        assertTrue(rs.getTime(8) == null && rs.wasNull());
        assertTrue(rs.getTimestamp(9) == null && rs.wasNull());
        assertTrue(rs.getDouble(10) == 0.0 && rs.wasNull());
        assertTrue(rs.getFloat(11) == 0.0 && rs.wasNull());
        assertTrue(rs.getLong(12) == 0 && rs.wasNull());
        assertTrue(rs.getObject(13) == null && rs.wasNull());
        assertTrue(rs.getShort(14) == 0 && rs.wasNull());
        assertTrue(rs.getCharacterStream(15) == null && rs.wasNull());
        assertTrue(rs.getBinaryStream(16) == null && rs.wasNull());

        rs.next();
        assertTrue(rs.getInt(1) == 1);
        assertTrue(rs.getString(2) == null && rs.wasNull());
        assertTrue(rs.getBigDecimal(3) == null && rs.wasNull());
        assertTrue(!rs.getBoolean(4) && !rs.wasNull());
        assertTrue(rs.getByte(5) == 0 && !rs.wasNull());
        assertTrue(rs.getBytes(6) == null && rs.wasNull());
        assertTrue(rs.getDate(7) == null && rs.wasNull());
        assertTrue(rs.getTime(8) == null && rs.wasNull());
        assertTrue(rs.getTimestamp(9) == null && rs.wasNull());
        assertTrue(rs.getDouble(10) == 0.0 && !rs.wasNull());
        assertTrue(rs.getFloat(11) == 0.0 && !rs.wasNull());
        assertTrue(rs.getLong(12) == 0 && !rs.wasNull());
        assertTrue(rs.getObject(13) == null && rs.wasNull());
        assertTrue(rs.getShort(14) == 0 && !rs.wasNull());
        Assert.assertEquals("test", rs.getString(15));
        Assert.assertEquals(new byte[]{(byte) 0xff, 0x00}, rs.getBytes(16));

        rs.next();
        assertTrue(rs.getInt(1) == 2);
        Assert.assertEquals("+", rs.getString(2));
        Assert.assertEquals("1.20", rs.getBigDecimal(3).toString());
        assertTrue(rs.getBoolean(4));
        assertTrue((rs.getByte(5) & 0xff) == 0xff);
        Assert.assertEquals(new byte[]{0x00, (byte) 0xff}, rs.getBytes(6));
        Assert.assertEquals("2005-09-21", rs.getDate(7).toString());
        Assert.assertEquals("21:46:28", rs.getTime(8).toString());
        Assert.assertEquals("2005-09-21 21:47:09.567890123", rs.getTimestamp(9).toString());
        assertTrue(rs.getDouble(10) == 1.725);
        assertTrue(rs.getFloat(11) == (float) 2.5);
        assertTrue(rs.getLong(12) == Long.MAX_VALUE);
        Assert.assertEquals(10, ((Integer) rs.getObject(13)).intValue());
        assertTrue(rs.getShort(14) == Short.MIN_VALUE);
        // auml ouml uuml
        Assert.assertEquals("\u00ef\u00f6\u00fc", rs.getString(15));
        Assert.assertEquals(new byte[]{(byte) 0xab, 0x12}, rs.getBytes(16));

        for (int i = 3; i <= 14; i++) {
            rs.next();
            Assert.assertEquals(i, rs.getInt(1));
            Assert.assertEquals("\u00ef\u00f6\u00fc", rs.getString(15));
            Assert.assertEquals(new byte[]{(byte) 0xab, 0x12}, rs.getBytes(16));
        }
        assertFalse(rs.next());

        stat.execute("DROP TABLE TEST");
        conn.close();
    }

    private void testUpdateDeleteInsert() throws SQLException {
        Connection c1 = getConnection();
        Connection c2 = getConnection();
        Statement stat = c1.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_UPDATABLE);
        stat.execute("DROP TABLE IF EXISTS TEST");
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255))");
        int max = 8;
        for (int i = 0; i < max; i++) {
            stat.execute("INSERT INTO TEST VALUES(" + i + ", 'Hello" + i + "')");
        }
        ResultSet rs;
        rs = stat.executeQuery("SELECT * FROM TEST ORDER BY ID");
        rs.next();
        Assert.assertEquals(0, rs.getInt(1));
        rs.moveToInsertRow();
        rs.updateInt(1, 100);
        rs.moveToCurrentRow();
        Assert.assertEquals(0, rs.getInt(1));

        rs = stat.executeQuery("SELECT * FROM TEST");
        int j = max;
        while (rs.next()) {
            int id = rs.getInt(1);
            if (id % 2 == 0) {
                Statement s2 = c2.createStatement();
                s2.execute("UPDATE TEST SET NAME = NAME || '+' WHERE ID = " + rs.getInt(1));
                if (id % 4 == 0) {
                    rs.refreshRow();
                }
                rs.updateString(2, "Updated " + rs.getString(2));
                rs.updateRow();
            } else {
                rs.deleteRow();
            }
            // the driver does not detect it in any case
            assertFalse(rs.rowUpdated());
            assertFalse(rs.rowInserted());
            assertFalse(rs.rowDeleted());

            rs.moveToInsertRow();
            rs.updateString(2, "Inserted " + j);
            rs.updateInt(1, j);
            j += 2;
            rs.insertRow();

            // the driver does not detect it in any case
            assertFalse(rs.rowUpdated());
            assertFalse(rs.rowInserted());
            assertFalse(rs.rowDeleted());

        }
        rs = stat.executeQuery("SELECT * FROM TEST ORDER BY ID");
        while (rs.next()) {
            int id = rs.getInt(1);
            String name = rs.getString(2);
            Assert.assertEquals(0, id % 2);
            if (id >= max) {
                Assert.assertEquals("Inserted " + id, rs.getString(2));
            } else {
                if (id % 4 == 0) {
                    Assert.assertEquals("Updated Hello" + id + "+", rs.getString(2));
                } else {
                    Assert.assertEquals("Updated Hello" + id, rs.getString(2));
                }
            }
            trace("id=" + id + " name=" + name);
        }
        c2.close();
        c1.close();

        // test scrollable result sets
        Connection conn = getConnection();
        for (int i = 0; i < 5; i++) {
            testScrollable(conn, i);
        }
        conn.close();
    }

    private void testScrollable(Connection conn, int rows) throws SQLException {
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE IF NOT EXISTS TEST" +
                "(ID INT PRIMARY KEY, NAME VARCHAR(255))");
        stat.execute("DELETE FROM TEST");
        PreparedStatement prep = conn.prepareStatement(
                "INSERT INTO TEST VALUES(?, ?)");
        for (int i = 0; i < rows; i++) {
            prep.setInt(1, i);
            prep.setString(2, "Data " + i);
            prep.execute();
        }
        Statement regular = conn.createStatement();
        testScrollResultSet(regular, ResultSet.TYPE_FORWARD_ONLY, rows);
        Statement scroll = conn.createStatement(
                ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        testScrollResultSet(scroll, ResultSet.TYPE_SCROLL_INSENSITIVE, rows);
    }

    private void testScrollResultSet(Statement stat, int type, int rows)
            throws SQLException {
        boolean error = false;
        if (type == ResultSet.TYPE_FORWARD_ONLY) {
            error = true;
        }
        ResultSet rs = stat.executeQuery("SELECT * FROM TEST");
        Assert.assertEquals(type, rs.getType());

        assertState(rs, rows > 0, false, false, false);
        for (int i = 0; i < rows; i++) {
            rs.next();
            assertState(rs, rows == 0, i == 0, i == rows - 1, rows == 0 || i == rows);
        }
        try {
            rs.beforeFirst();
            assertState(rs, rows > 0, false, false, false);
        } catch (SQLException e) {
            if (!error) {
                throw e;
            }
        }
        try {
            rs.afterLast();
            assertState(rs, false, false, false, rows > 0);
        } catch (SQLException e) {
            if (!error) {
                throw e;
            }
        }
        try {
            boolean valid = rs.first();
            Assert.assertEquals(rows > 0, valid);
            if (valid) {
                assertState(rs, false, true, rows == 1, rows == 0);
            }
        } catch (SQLException e) {
            if (!error) {
                throw e;
            }
        }
        try {
            boolean valid = rs.last();
            Assert.assertEquals(rows > 0, valid);
            if (valid) {
                assertState(rs, false, rows == 1, true, rows == 0);
            }
        } catch (SQLException e) {
            if (!error) {
                throw e;
            }
        }
    }

    private void assertState(ResultSet rs, boolean beforeFirst,
                             boolean first, boolean last, boolean afterLast) throws SQLException {
        Assert.assertEquals(beforeFirst, rs.isBeforeFirst());
        Assert.assertEquals(first, rs.isFirst());
        Assert.assertEquals(last, rs.isLast());
        Assert.assertEquals(afterLast, rs.isAfterLast());
    }

}
