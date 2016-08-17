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

package com.openddal.test.explain;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.Test;

import com.openddal.test.BaseTestCase;
import com.openddal.util.JdbcUtils;

/**
 * @author jorgie.li
 */
public class ExplainTestCase extends BaseTestCase {


    @Test
    public void tesetCreateTableExplain() throws Exception {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            conn = getConnection();
            stmt = conn.createStatement();
            rs = stmt.executeQuery("EXPLAIN PLAN FOR CREATE TABLE IF NOT EXISTS `order_items`(`item_id` int(11) NOT NULL,`order_id` int(11) NOT NULL,`item_info` varchar(218) DEFAULT NULL,`create_date` datetime NOT NULL, PRIMARY KEY (`order_id`),  KEY (`create_date`), FOREIGN KEY (`order_id`) REFERENCES `orders` (`order_id`)) ENGINE=InnoDB DEFAULT CHARSET=latin1");
            printResultSet(rs);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            JdbcUtils.closeSilently(rs);
            JdbcUtils.closeSilently(stmt);
            JdbcUtils.closeSilently(conn);
        }
    }
    @Test
    public void tesetDropTableExplain() throws Exception {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            conn = getConnection();
            stmt = conn.createStatement();
            rs = stmt.executeQuery("EXPLAIN PLAN FOR DROP TABLE IF EXISTS customers,address,order_items,order_status,orders,product,product_category,customer_login_log");
            printResultSet(rs);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            JdbcUtils.closeSilently(rs);
            JdbcUtils.closeSilently(stmt);
            JdbcUtils.closeSilently(conn);
        }
    }

    @Test
    public void tesetAlterTableExplain() throws Exception {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            conn = getConnection();
            stmt = conn.createStatement();
            rs = stmt.executeQuery("EXPLAIN PLAN FOR ALTER TABLE  CUSTOMERS ADD IF NOT EXISTS GMT_TIME TIME");
            printResultSet(rs);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            JdbcUtils.closeSilently(rs);
            JdbcUtils.closeSilently(stmt);
            JdbcUtils.closeSilently(conn);
        }
    }

}
