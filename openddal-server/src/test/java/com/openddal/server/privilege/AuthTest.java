package com.openddal.server.privilege;

import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.junit.Assert;
import org.junit.Test;


/**
 * Created by snow_young on 16/7/16.
 */
public class AuthTest {
    private String ddalUrl = "jdbc:mysql://localhost:6100/schema_main?connectTimeout=1000&amp;rewriteBatchedStatements=true" ;
    @Test
    public void authDDAL() throws UnsupportedEncodingException, NoSuchAlgorithmException {
        try{
            //加载MySql的驱动类
            Class.forName("com.mysql.jdbc.Driver") ;
        }catch(ClassNotFoundException e){
            System.out.println("找不到驱动程序类 ，加载驱动失败！");
            e.printStackTrace() ;
        }

        String username = "root" ;
        String password = "X$re58i4klhfg" ;
        String failPassword = "xujianhai123";
        // test success
        Connection con = null;
        try{
            con = DriverManager.getConnection(ddalUrl, username, password) ;
        }catch(SQLException se){
            System.out.println("数据库连接失败！");
            se.printStackTrace() ;
            Assert.assertTrue(false);
        }finally{
            if(con != null) {
                try {
                    con.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

        // test fail
        try{
            con = DriverManager.getConnection(ddalUrl, username, failPassword);
            Assert.assertTrue(false);
        }catch(SQLException se) {
            System.out.println("数据库连接应该失败！符合预期");
            se.printStackTrace();
        }finally {
            if(con != null){
                try {
                    con.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }




}
