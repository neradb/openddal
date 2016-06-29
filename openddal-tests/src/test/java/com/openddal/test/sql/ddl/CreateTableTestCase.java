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
package com.openddal.test.sql.ddl;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Test;

import com.openddal.test.BaseTestCase;

/**
 * Created by jorgie.li on 2015/11/27.
 */
public class CreateTableTestCase extends BaseTestCase {

    public static final String CONSTRAINT_PARENT_SQL = "CREATE TABLE  %s (   `col1` int(11) NOT NULL,   `col2` int(11) DEFAULT NULL,   `col3` varchar(20) DEFAULT NULL,   `col4` varchar(200) DEFAULT NULL,   `col5` int(1) DEFAULT 0,   PRIMARY KEY(`col1`),  UNIQUE KEY(`col2`),   KEY(`col3`),   CHECK (`col5` IN (1,2,3,4,5))  ) ENGINE=InnoDB DEFAULT CHARSET=latin1";
    public static final String CONSTRAINT_CHILD_SQL = "CREATE TABLE  %s (   `col1` int(11) NOT NULL,   `col2` int(11) NOT NULL,   `col3` varchar(200) DEFAULT NULL,   `col4` datetime NOT NULL,   `col5` int(1) DEFAULT 0,   PRIMARY KEY (`col1`),   UNIQUE KEY (`col2`),   FOREIGN KEY (`col3`) REFERENCES `parent` (`col4`),   KEY(`col4`),   CHECK (`col5` IN (1,2,3,4,5)) ) ENGINE=InnoDB DEFAULT CHARSET=latin1";
    public static final String DATA_TYPE_SQL = "CREATE TABLE %s (   `col1` INT NOT NULL,   `col2` VARCHAR(45) NULL,   `col3` DECIMAL(4) NOT NULL,   `col4` DATETIME NULL,   `col5` BLOB NULL,   `col6` DATE NULL,   `col7` DOUBLE NULL,   `col8` TINYINT(1) NOT NULL DEFAULT 0,   `col9` SMALLINT(1) NULL,   `col10` CHAR(10) NULL,   `col11` NVARCHAR(10) NOT NULL DEFAULT 'test',   `col12` YEAR NOT NULL DEFAULT 2001,   `col13` BIGINT NULL,   `col14` BINARY NULL,   `col15` TINYBLOB NULL,   `col16` MEDIUMBLOB NULL,   `col17` LONGBLOB NULL,   `col18` TINYTEXT NULL,   `col19` LONGTEXT NULL,   `col20` TEXT NULL,   PRIMARY KEY (`col1`)   ) ENGINE=InnoDB DEFAULT CHARSET=latin1";
    public static String DROP_STMT_IF = "DROP TABLE IF EXISTS %s";
    public static String DROP_SQL = "DROP TABLE %s";
    private Connection conn;
    private Statement stmt;

    public CreateTableTestCase() {
        System.setProperty("ddal.engineConfigLocation", "ddal-engine.xml");
    }

    @Test
    public void test() throws Exception {
        try {
            conn = getConnection();
            stmt = conn.createStatement();
            testDataTypes("`test`");
            testConstraint("parent", "child");
            testConstraint("`replication_parent`", "`replication_child`");
            testConstraint("`partition_parent`", "`partition_child`");
        } finally {
            close(conn, stmt, null);
        }
    }

    private void testDataTypes(String table) throws SQLException {
        stmt.execute(String.format(DROP_STMT_IF, table));
        stmt.execute(String.format(DATA_TYPE_SQL, table));
        stmt.execute(String.format(DROP_SQL, table));

    }

    private void testConstraint(String parent, String child) throws SQLException {
        stmt.execute(String.format(DROP_STMT_IF, parent));
        stmt.execute(String.format(DROP_STMT_IF, child));
        stmt.execute(String.format(CONSTRAINT_PARENT_SQL, parent));
        stmt.execute(String.format(CONSTRAINT_CHILD_SQL, child));
        stmt.execute(String.format(DROP_SQL, parent));
        stmt.execute(String.format(DROP_SQL, child));
    }

}
