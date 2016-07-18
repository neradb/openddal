package com.openddal.server.privilege;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.Assert;
import org.junit.Test;

import com.openddal.server.mysql.auth.Privilege;
import com.openddal.server.mysql.auth.PrivilegeDefault;

/**
 * Created by snow_young on 16/7/17.
 */
public class PrivilegeTest {

    @Test
    public void testPropPrivilege(){
        Privilege privilege = PrivilegeDefault.getPrivilege();

        // Make test data
        Map<String, String> users = new HashMap<String, String>();
        users.put("root", "X$re58i4klhfg");
        users.put("mysql", "$%RWi4klhfg");
        users.put("app01", "%ERDFSDhfg");
        Properties prop = new Properties();
        prop.putAll(users);
        
        // test get
        Assert.assertEquals(users.get("root"), privilege.password("root"));
        Assert.assertEquals(users.get("mysql"), privilege.password("mysql"));

        // test check
        // params: the clientName clientPass asalt is accquired from the mysql-connector-java-5.1.29 auth data stream.
        Assert.assertTrue(privilege.checkPassword("root", "X$re58i4klhfg", "12345678"));
    }

}
