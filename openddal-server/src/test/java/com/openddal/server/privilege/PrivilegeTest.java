package com.openddal.server.privilege;

import com.openddal.server.mysql.auth.PropPrivilege;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by snow_young on 16/7/17.
 */
public class PrivilegeTest {

    @Test
    public void testPropPrivilege(){
        String propPath = System.getProperty("user.dir") + "/src/test/resources/config/prop.properties";
        PropPrivilege privilege = new PropPrivilege(propPath);

        // Make test data
        Map<String, String> users = new HashMap<String, String>();
        users.put("xujianhai", "xujianhaimima");
        users.put("zhangsan", "zhangsanmima");
        users.put("root", "xujianhai");
        Properties prop = new Properties();
        prop.putAll(users);
        try {
            FileOutputStream fos = new FileOutputStream(new File(propPath));
            prop.store(fos, "test for users");
        } catch (IOException e) {
            Assert.assertTrue(false);
        }

        privilege.init();

        // test get
        Assert.assertEquals(users.get("xujianhai"), privilege.get("xujianhai"));
        Assert.assertEquals(users.get("zhangsan"), privilege.get("zhangsan"));

        // test check
        // params: the clientName clientPass asalt is accquired from the mysql-connector-java-5.1.29 auth data stream.
        Assert.assertTrue(privilege.hasPrivilege("root", "yRn3ay1trCLPELEsYD2C5Yi9Ytk=", "12345678"));
    }

}
