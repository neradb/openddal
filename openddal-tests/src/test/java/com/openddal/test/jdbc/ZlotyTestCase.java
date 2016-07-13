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

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.Test;

import com.openddal.message.ErrorCode;
import com.openddal.test.BaseTestCase;

import junit.framework.Assert;

/**
 * Tests a custom BigDecimal implementation, as well
 * as direct modification of a byte in a byte array.
 */
public class ZlotyTestCase extends BaseTestCase {


    @Test
    public void test() throws SQLException {
        testZloty();
        testModifyBytes();

    }

    private void testModifyBytes() throws SQLException {

        Connection conn = getConnection();
        conn.createStatement().execute(
                "CREATE TABLE TEST(ID INT, DATA BINARY)");
        PreparedStatement prep = conn.prepareStatement(
                "INSERT INTO TEST VALUES(?, ?)");
        byte[] shared = {0};
        prep.setInt(1, 0);
        prep.setBytes(2, shared);
        prep.execute();
        shared[0] = 1;
        prep.setInt(1, 1);
        prep.setBytes(2, shared);
        prep.execute();
        ResultSet rs = conn.createStatement().executeQuery(
                "SELECT * FROM TEST ORDER BY ID");
        rs.next();
        Assert.assertEquals(0, rs.getInt(1));
        Assert.assertEquals(0, rs.getBytes(2)[0]);
        rs.next();
        Assert.assertEquals(1, rs.getInt(1));
        Assert.assertEquals(1, rs.getBytes(2)[0]);
        rs.getBytes(2)[0] = 2;
        Assert.assertEquals(1, rs.getBytes(2)[0]);
        Assert.assertFalse(rs.next());
        conn.close();
    }

    /**
     * H2 destroyer application ;->
     *
     * @author Maciej Wegorkiewicz
     */
    private void testZloty() throws SQLException {

        Connection conn = getConnection();
        conn.createStatement().execute("CREATE TABLE TEST(ID INT, AMOUNT DECIMAL)");
        PreparedStatement prep = conn.prepareStatement("INSERT INTO TEST VALUES(?, ?)");
        prep.setInt(1, 1);
        prep.setBigDecimal(2, new BigDecimal("10.0"));
        prep.execute();
        prep.setInt(1, 2);
        assertThrows(ErrorCode.INVALID_CLASS_2, prep).
                setBigDecimal(2, new ZlotyBigDecimal("11.0"));

        prep.setInt(1, 3);
        BigDecimal value = new BigDecimal("12.100000") {

            private static final long serialVersionUID = 1L;

            @Override
            public String toString() {
                return "12,100000 EURO";
            }
        };
        assertThrows(ErrorCode.INVALID_CLASS_2, prep).
                setBigDecimal(2, value);

        conn.close();
    }

    /**
     * This class overrides BigDecimal and implements some strange comparison
     * method.
     */
    private static class ZlotyBigDecimal extends BigDecimal {

        private static final long serialVersionUID = 1L;

        public ZlotyBigDecimal(String s) {
            super(s);
        }

        @Override
        public int compareTo(BigDecimal bd) {
            return -super.compareTo(bd);
        }

    }

}
