package com.openddal.config;

import java.io.Serializable;
import java.util.Properties;

public class SequenceRule implements Serializable{
    private static final long serialVersionUID = 1L;
    private String name;
    private String strategy;
    private Properties properties;

    public SequenceRule(String name) {
        this.name = name;
    }
    
    public String getName() {
        return name;
    }
    
    public void setName(String name) {
        this.name = name;
    }

    public String getStrategy() {
        return strategy;
    }

    public void setStrategy(String strategy) {
        this.strategy = strategy;
    }

    public Properties getProperties() {
        return properties;
    }
    public void setProperties(Properties properties) {
        this.properties = properties;
    }
    
    
    
}
