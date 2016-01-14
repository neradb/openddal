package com.openddal.server.test;

import org.junit.Test;

import com.openddal.server.ServerLauncher;

public class NettyServerTestCase {
    
    @Test
    public void main() {
        System.setProperty("ddal.stdout", "true");
        System.setProperty("ddal.flowlog", "true");
        String[] args = new String[]{
                "-port","6100",
                "-ssl","false",
                "-maxThreads","500",
                "-sendBuff","1024",
                "-recvBuff","1024",
                "protocol","mysql",
                "configFile","",
                
        };
        ServerLauncher.launch(args);
    }
    
     
    
    
}
