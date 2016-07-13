package com.openddal.test.sql.dml;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.Test;

import com.openddal.test.BaseTestCase;
import com.openddal.util.JdbcUtils;

import junit.framework.Assert;
public class JoinFreeTestCase extends BaseTestCase{
    
    @Test
    public void tesetTableGroupInnerJoin() throws Exception {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            conn = getConnection();
            stmt = conn.createStatement();
            String sql = "select * from customers a,address b where a.id=b.customer_id and b.customer_id=100";
            rs = stmt.executeQuery("explain plan for " + sql);
            rs.next();
            String plan = rs.getString(1);
            System.out.println(plan);
            rs.close();
            rs = stmt.executeQuery(sql);
            printResultSet(rs);
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

}
