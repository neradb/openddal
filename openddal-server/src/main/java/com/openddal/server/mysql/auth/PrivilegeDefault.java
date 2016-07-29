package com.openddal.server.mysql.auth;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.openddal.engine.SysProperties;
import com.openddal.server.util.SecurityUtil;
import com.openddal.server.util.StringUtil;
import com.openddal.util.Utils;

/**
 * Created by snow_young on 16/7/17.
 */
public class PrivilegeDefault implements Privilege {
    
    private static final Logger logger = LoggerFactory.getLogger(PrivilegeDefault.class);
        
    private static final Privilege TRUE_PRIVILEGE = new Privilege(){
        @Override
        public boolean userExists(String user) {
            return true;
        }
        @Override
        public boolean schemaExists(String user, String schema) {
            return true;
        }
        @Override
        public String password(String user) {
            return null;
        }
        @Override
        public boolean checkPassword(String user, String password, String salt) {
            return true;
        }
    };


    private final Properties prop;

    private PrivilegeDefault(Properties users) {
        this.prop = users;
    }

    @Override
    public boolean userExists(String user) {
        String property = prop.getProperty("users");
        String[] users = StringUtil.split(property, ',', true);
        return Arrays.asList(users).contains(user);
    }

    @Override
    public boolean schemaExists(String user, String schema) {
        String property = prop.getProperty(user + ".schemas");
        String[] schemas = StringUtil.split(property, ',', true);
        return Arrays.asList(schemas).contains(schema);
    }

    @Override
    public String password(String user) {
        return prop.getProperty(user + ".password");
    }

    @Override
    public boolean checkPassword(String user, String password, String salt) {

        String localPass = password(user);
        try {
            if(StringUtil.isEmpty(localPass) && StringUtil.isEmpty(password)){
                return true;
            }
            byte[] scramble411 = SecurityUtil.scramble411(localPass, salt);
            String encryptPass411 = new String(scramble411,"UTF-8");
            if(encryptPass411.equals(password)){
                return true;
            }
        } catch (Exception e) {
            logger.info("validateUserPassword error", e);
        }
        return false;
    }
    
    public static Privilege getPrivilege() {
        String path = SysProperties.SERVERUSER_CONFIG_LOCATION;
        InputStream source = Utils.getResourceAsStream(path);
        if(source == null) {
            logger.debug("Can't load privilege config from {}, using ", path);
            return TRUE_PRIVILEGE;
        }
        try {
            logger.debug("Using privilege config from {}" , path);
            Properties prop = new Properties();
            prop.load(source);
            return new PrivilegeDefault(prop);
        } catch (Exception e) {
            logger.info("error load privilege config from " + path, e);
            return TRUE_PRIVILEGE;
        }
    
    }
}
