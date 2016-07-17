package com.openddal.server.frontend;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by snow_young on 16/7/17.
 */
public class PropPrivilege extends AbstractPrivilege{
    private static final Logger logger = LoggerFactory.getLogger(PropPrivilege.class);

    private String filePath = "";
    private Properties prop;

    public PropPrivilege(String filePath){
        this.filePath = filePath;
        prop = new Properties();
    }

    @Override
    public void init() {
        try {
            prop.load(new FileReader(filePath));
        } catch (IOException e) {
            logger.error("init failed ", e);
//            should stop here
        }
    }

    // whether the concurrent problem?
    @Override
    public void reload(){
        try {
            prop.load(new FileReader(filePath));
        } catch (IOException e) {
//            if failed, what should do.
            logger.error("reload failed", e);
        }
    }

    public String get(String name){
        return (String) prop.get(name);
    }
}
