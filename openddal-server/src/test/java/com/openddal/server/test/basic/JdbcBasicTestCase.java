package com.openddal.server.test.basic;

import org.apache.commons.dbcp.BasicDataSource;

import com.openddal.test.BaseTestCase;

public class JdbcBasicTestCase extends BaseTestCase {
    
    private String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
    
    private JdbcBasicTestCase() {
        BasicDataSource datasource = new BasicDataSource();
        datasource.setMaxActive(10000);
        datasource.setValidationQuery("select 1");
        datasource.setDriverClassName(MYSQL_DRIVER);
        datasource.setUrl("jdbc:mysql://localhost:6100/ddal_db1?connectTimeout=1000&amp;rewriteBatchedStatements=true");
        datasource.setUsername("username");
        datasource.setPassword("password");
        setDataSource(datasource);
    }

}
