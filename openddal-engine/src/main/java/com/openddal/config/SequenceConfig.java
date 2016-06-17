package com.openddal.config;

import java.io.Serializable;
import java.util.Properties;

public class SequenceConfig implements Serializable{
    private static final long serialVersionUID = 1L;
    private String name;
    private Properties properties;

    public SequenceConfig(String name) {
        this.name = name;
    }
    
    public String getName() {
        return name;
    }

    public Properties getProperties() {
        return properties;
    }
    public void setProperties(Properties properties) {
        this.properties = properties;
    }
    
    
    
}
