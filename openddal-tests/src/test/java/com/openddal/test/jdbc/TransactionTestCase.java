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

import java.sql.Connection;
import java.sql.SQLException;

import org.junit.Test;

import com.openddal.jdbc.JdbcConnection;
import com.openddal.message.ErrorCode;
import com.openddal.test.BaseTestCase;

import junit.framework.Assert;

/**
 * Transaction isolation level tests.
 */
public class TransactionTestCase extends BaseTestCase {

    private Connection conn1, conn2;

    @Test
    public void test() throws Exception {
        setQueryTimeout();
        testSetAutoCommit();
        testSetReadOnly();
        testsetIsolationLevel();
        testTableLevelLocking();
    }

    private void testTableLevelLocking() throws SQLException {
        conn1 = getConnection();
        Assert.assertEquals(Connection.TRANSACTION_READ_COMMITTED,
                conn1.getTransactionIsolation());
        conn1.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        Assert.assertEquals(Connection.TRANSACTION_SERIALIZABLE,
                conn1.getTransactionIsolation());
        conn1.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
        Assert.assertEquals(Connection.TRANSACTION_READ_UNCOMMITTED,
                conn1.getTransactionIsolation());
        assertSingleValue(conn1.createStatement(), "CALL LOCK_MODE()", 0);
        conn1.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        assertSingleValue(conn1.createStatement(), "CALL LOCK_MODE()", 3);
        Assert.assertEquals(Connection.TRANSACTION_READ_COMMITTED,
                conn1.getTransactionIsolation());
        conn1.createStatement().execute("SET LOCK_MODE 1");
        Assert.assertEquals(Connection.TRANSACTION_SERIALIZABLE,
                conn1.getTransactionIsolation());
        conn1.createStatement().execute("CREATE TABLE TEST(ID INT)");
        conn1.createStatement().execute("INSERT INTO TEST VALUES(1)");
        conn1.setAutoCommit(false);

        conn2 = getConnection();
        conn2.setAutoCommit(false);

        conn1.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

        // serializable: just reading
        assertSingleValue(conn1.createStatement(), "SELECT * FROM TEST", 1);
        assertSingleValue(conn2.createStatement(), "SELECT * FROM TEST", 1);
        conn1.commit();
        conn2.commit();

        // serializable: write lock
        conn1.createStatement().executeUpdate("UPDATE TEST SET ID=2");
        assertThrows(ErrorCode.LOCK_TIMEOUT_1, conn2.createStatement()).
                executeQuery("SELECT * FROM TEST");
        conn1.commit();
        conn2.commit();

        conn1.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

        // read-committed: #1 read, #2 update, #1 read again
        assertSingleValue(conn1.createStatement(), "SELECT * FROM TEST", 2);
        conn2.createStatement().executeUpdate("UPDATE TEST SET ID=3");
        conn2.commit();
        assertSingleValue(conn1.createStatement(), "SELECT * FROM TEST", 3);
        conn1.commit();

        // read-committed: #1 read, #2 read, #2 update, #1 delete
        assertSingleValue(conn1.createStatement(), "SELECT * FROM TEST", 3);
        assertSingleValue(conn2.createStatement(), "SELECT * FROM TEST", 3);
        conn2.createStatement().executeUpdate("UPDATE TEST SET ID=4");
        assertThrows(ErrorCode.LOCK_TIMEOUT_1, conn1.createStatement()).
                executeUpdate("DELETE FROM TEST");
        conn2.commit();
        conn1.commit();
        assertSingleValue(conn1.createStatement(), "SELECT * FROM TEST", 4);
        assertSingleValue(conn2.createStatement(), "SELECT * FROM TEST", 4);

        conn1.close();
        conn2.close();
    }


    public void setQueryTimeout() throws Exception {
        JdbcConnection conn = null;
        try {
            conn = (JdbcConnection) getConnection();
            conn.setQueryTimeout(10);
        } finally {
            close(conn, null, null);
        }
    }

    public void testSetAutoCommit() throws Exception {
        Connection conn = null;
        try {
            conn = getConnection();
            conn.setAutoCommit(false);
            conn.setAutoCommit(true);
        } finally {
            close(conn, null, null);
        }
    }


    public void testSetReadOnly() throws Exception {
        Connection conn = null;
        try {
            conn = getConnection();
            conn.setReadOnly(false);
            conn.setReadOnly(true);
        } finally {
            close(conn, null, null);
        }
    }

    public void testsetIsolationLevel() throws Exception {
        Connection conn = null;
        try {
            conn = getConnection();
            conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
            conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

        } catch (Exception e) {

        } finally {
            close(conn, null, null);
        }
    }

    @Test
    public void performance() throws Exception {
        Connection conn = null;
        try {
            long start = System.currentTimeMillis();

            for (int i = 0; i < 10000; i++) {
                conn = getConnection();
                conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
                close(conn, null, null);
            }

            long end = System.currentTimeMillis();
            System.out.println("cost " + (end - start) + "ms.");
            /*
            start = System.currentTimeMillis();
            for (int i = 0; i < 10000; i++) {
                conn = getConnection();
                conn.getTransactionIsolation();
                close(conn, null, null);
            }
            end = System.currentTimeMillis();
            System.out.println("cost " + (end - start) + "ms.");
            */

        } catch (Exception e) {

        } finally {
            close(conn, null, null);
        }
    }

}
