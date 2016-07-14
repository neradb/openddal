package com.openddal.test.sql.dml;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.openddal.test.BaseTestCase;
import com.openddal.util.JdbcUtils;

import junit.framework.Assert;
public class JoinFreeTestCase extends BaseTestCase{
    
    @BeforeClass
    public static void setUp() {
        url = "jdbc:openddal:conf/JoinFree.xml;";
    }
    
    
    @Before
    public void initTest() throws Exception {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            conn = getConnection();
            stmt = conn.createStatement();
            stmt.execute("CREATE TABLE IF NOT EXISTS g1_table1(id int(11) not null, rand_id int(11) default null, name varchar(20) default null, primary key (id),  key (rand_id))");
            stmt.execute("CREATE TABLE IF NOT EXISTS g1_table2(id int(11) not null, ref_id int(11) default null, rand_id int(11) default null, name varchar(20) default null, primary key (id),  key (rand_id))");
            
            stmt.execute("CREATE TABLE IF NOT EXISTS g2_table1(id int(11) not null, main_id int(11) default null, rand_id int(11) default null, name varchar(20) default null, primary key (id),  key (rand_id))");
            stmt.execute("CREATE TABLE IF NOT EXISTS g2_table2(id int(11) not null, main_id int(11) default null, ref_id int(11) default null, rand_id int(11) default null, name varchar(20) default null, primary key (id),  key (rand_id))");
            stmt.execute("CREATE TABLE IF NOT EXISTS g2_table3(id int(11) not null, main_id int(11) default null, ref_id int(11) default null, rand_id int(11) default null, name varchar(20) default null, primary key (id),  key (rand_id))");
            
            stmt.execute("CREATE TABLE IF NOT EXISTS global1(id int(11) not null, rand_id int(11) default null, name varchar(20) default null, primary key (id),  key (rand_id))");
            stmt.execute("CREATE TABLE IF NOT EXISTS global1(id int(11) not null, ref_id int(11) default null, rand_id int(11) default null, name varchar(20) default null, primary key (id),  key (rand_id))");

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
    public void tesetTableGroupInnerJoin() throws Exception {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            conn = getConnection();
            stmt = conn.createStatement();
            String sql = "select * from g1_table1 a,g1_table2 b where a.id=b.ref_id and b.ref_id=100";
            rs = stmt.executeQuery("explain plan for " + sql);
            rs.next();
            String plan = rs.getString(1);
            assertContains(plan, "SINGLE_EXECUTION");
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
    
    
    @Test
    public void tesetTableGroupOutJoin() throws Exception {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            conn = getConnection();
            stmt = conn.createStatement();
            String sql = "select * from g1_table1 a left join g1_table2 b on a.id=b.ref_id where b.ref_id=100";
            rs = stmt.executeQuery("explain plan for " + sql);
            rs.next();
            String plan = rs.getString(1);
            assertContains(plan, "SINGLE_EXECUTION");
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
