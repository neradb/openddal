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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.Test;

import com.openddal.test.BaseTestCase;

import junit.framework.Assert;

/**
 * Test for limit updates.
 */
public class LimitUpdatesTestCase extends BaseTestCase {


    private static int countWhere(final Connection conn, final int where)
            throws SQLException {
        PreparedStatement prep = null;
        ResultSet rs = null;
        try {
            prep = conn.prepareStatement(
                    "SELECT COUNT(*) FROM TEST WHERE VALUE_ID = ?");
            prep.setInt(1, where);
            rs = prep.executeQuery();
            rs.next();
            return rs.getInt(1);
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (prep != null) {
                prep.close();
            }
        }
    }

    private static void updateLimit(final Connection conn, final int value,
                                    final int limit) throws SQLException {
        PreparedStatement prep = null;
        try {
            prep = conn.prepareStatement(
                    "UPDATE TEST SET VALUE_ID = ? LIMIT ?");
            prep.setInt(1, value);
            prep.setInt(2, limit);
            prep.execute();
        } finally {
            if (prep != null) {
                prep.close();
            }
        }
    }

    @Test
    public void test() throws SQLException {
        testLimitUpdates();
    }

    private void testLimitUpdates() throws SQLException {
        Connection conn = null;
        PreparedStatement prep = null;

        try {
            conn = getConnection();
            prep = conn.prepareStatement(
                    "CREATE TABLE TEST(KEY_ID INT PRIMARY KEY, VALUE_ID INT)");
            prep.executeUpdate();

            prep.close();
            prep = conn.prepareStatement("INSERT INTO TEST VALUES(?, ?)");
            int numRows = 10;
            for (int i = 0; i < numRows; ++i) {
                prep.setInt(1, i);
                prep.setInt(2, 0);
                prep.execute();
            }
            Assert.assertEquals(numRows, countWhere(conn, 0));

            // update all elements than available
            prep.close();
            prep = conn.prepareStatement("UPDATE TEST SET VALUE_ID = ?");
            prep.setInt(1, 1);
            prep.execute();
            Assert.assertEquals(numRows, countWhere(conn, 1));

            // update less elements than available
            updateLimit(conn, 2, numRows / 2);
            Assert.assertEquals(numRows / 2, countWhere(conn, 2));

            // update more elements than available
            updateLimit(conn, 3, numRows * 2);
            Assert.assertEquals(numRows, countWhere(conn, 3));

            // update no elements
            updateLimit(conn, 4, 0);
            Assert.assertEquals(0, countWhere(conn, 4));
        } finally {
            if (prep != null) {
                prep.close();
            }
            if (conn != null) {
                conn.close();
            }
        }
    }
}
