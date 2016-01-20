package com.openddal.server.test;

import org.junit.Test;

import com.openddal.server.ServerLauncher;

public class NettyServerTestCase {
    
    @Test
    public void main() {
        System.setProperty("engine.logfile", "logs/engine");
        System.setProperty("access.logfile", "logs/access");
        System.setProperty("server.logfile", "logs/server");
        System.setProperty("server.stdout", "true");
        System.setProperty("engine.stdout", "true");
        System.setProperty("server.flowlog", "true");
        System.setProperty("engine.flowlog", "true");
        System.setProperty("server.accesslog", "true");
        String[] args = new String[]{
                "-port","6100",
                "-ssl","false",
                "-maxThreads","500",
                "-sendBuff","1024",
                "-recvBuff","1024",
                "-protocol","mysql",
                "-configFile","",
                
        };
        ServerLauncher.launch(args);
    }
    
     
    
    
}
