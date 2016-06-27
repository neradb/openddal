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
// Created on 2015年4月7日
// $Id$

package com.openddal.test.sequence;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.Assert;
import org.junit.Test;

import com.openddal.test.BaseTestCase;
import com.openddal.util.JdbcUtils;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class SequenceTestCase extends BaseTestCase {


    @Test
    public void tesetQuerySeqValue() throws Exception {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            conn = getConnection();
            stmt = conn.createStatement();
            rs = stmt.executeQuery("select customer_seq.nextval dual");
            rs.next();
            System.out.println(" id " + rs.getLong(1));
            rs.close();
        
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            JdbcUtils.closeSilently(rs);
            JdbcUtils.closeSilently(stmt);
            JdbcUtils.closeSilently(conn);
        }
    }
    @Test
    public void tesetInsertSeqValue() throws Exception {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            conn = getConnection();
            stmt = conn.createStatement();
            for (int i = 0; i < 100; i++) {
                stmt.executeUpdate(
                        "insert into CUSTOMERS values(customer_seq.nextval, 1000, '马云', '大老', '1965-01-20')");
                rs = stmt.getGeneratedKeys();
                rs.next();
                System.out.println("LAST_INSERT_ID: " + rs.getLong(1));
                rs.close();
            }
            conn.commit();
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            JdbcUtils.closeSilently(rs);
            JdbcUtils.closeSilently(stmt);
            JdbcUtils.closeSilently(conn);
        }
    }

}
