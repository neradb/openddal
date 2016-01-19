package com.openddal.server.test;

import java.sql.Connection;

import org.apache.commons.dbcp.BasicDataSource;
import org.junit.Before;
import org.junit.Test;


public class MySQLJDBCTestCase {
    
    private String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
    private BasicDataSource datasource;
    
    @Before
    public void init() {
        datasource = new BasicDataSource();
        datasource.setMaxActive(10000);
        datasource.setValidationQuery("select 1");
        datasource.setDriverClassName(MYSQL_DRIVER);
        datasource.setUrl("jdbc:mysql://localhost:6100/ddal_db1?connectTimeout=1000&amp;rewriteBatchedStatements=true");
        datasource.setUsername("root");
        datasource.setPassword("admin");
    }
    
    @Test
    public void testConnection() throws Exception {
        try {
            Connection connection = datasource.getConnection();
            connection.setReadOnly(true);
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
