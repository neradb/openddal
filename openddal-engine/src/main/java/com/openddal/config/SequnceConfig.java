package com.openddal.config;

import java.io.Serializable;
import java.util.Properties;

public class SequnceConfig extends TableRule implements Serializable{
    private static final long serialVersionUID = 1L;
    
    private Properties properties;

    public SequnceConfig(String name) {
        super(name);
    }
    public Properties getProperties() {
        return properties;
    }
    public void setProperties(Properties properties) {
        this.properties = properties;
    }
    
    
    
}
